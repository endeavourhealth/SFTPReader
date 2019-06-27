package org.endeavourhealth.sftpreader.implementations.emis.utility;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.StringMemorySaver;
import org.endeavourhealth.sftpreader.implementations.emis.EmisBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class EmisFixDisabledService {
    private static final Logger LOG = LoggerFactory.getLogger(EmisFixDisabledService.class);

    private final EmisOrganisationMap org;
    private final DataLayerI db;
    private final DbInstanceEds instanceConfiguration;
    private final DbConfiguration dbConfiguration;

    private String tempDir = null;
    private List<Batch> batches = null;
    private Map<Batch, BatchSplit> hmBatchSplits = null;
    private int indexDisabled = -1;
    private int indexRebulked = -1;
    private int indexOriginallyBulked = -1;
    private Map<BatchSplit, List<BatchFile>> hmNewFilesCreated = new HashMap<>();
    private Map<String, String[]> hmFileHeadersByType = new HashMap<>();
    private Set<String> patientGuidsDeletedOrTooOld = null;

    public EmisFixDisabledService(EmisOrganisationMap org, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) {
        this.org = org;
        this.db = db;
        this.instanceConfiguration = instanceConfiguration;
        this.dbConfiguration = dbConfiguration;
    }

    /**
     * fixes Emis extract(s) when a practice was disabled then subsequently re-bulked, by
     * replacing the "delete" extract and any interim "disabled" ones with newly generated deltas that can be processed
     * before the re-bulk is done.
     *
     * This means we don't need to process the mass delete but also don't miss out of any manual user deletes of
     * data in the interim. Note, we still process the re-bulk - just not the mass delete.
     */
    public void fixDisabledExtract() throws Exception {
        LOG.info("Fixing Disabled Emis Extracts Prior to Re-bulk for service " + org.getOdsCode() + " " + org.getName());

        createTempDir();

        //get all batches for the config
        retrieveBatches();

        //go back through them to find the extract where the re-bulk is and when it was disabled (the list is in date order, so we're iterating most-recent first)
        findDisableAndRebulk();

        //work out if disabled old way or new way
        boolean oldWay = werePatientsDeleted();
        if (oldWay) {
            fixDisabledExtractOldWay();
        } else {
            fixDisabledExtractNewWay();
        }
    }

    /**
     * checks to see if we received a "delete" for patients on the date the extract became disabled,
     * which tells us if the service was disabled/fixed in the new way or old way
     */
    private boolean werePatientsDeleted() throws Exception {
        Batch disabledBatch = batches.get(indexDisabled);
        BatchSplit split = hmBatchSplits.get(disabledBatch);
        BatchFile batchFile = findBatchFile(disabledBatch, "Admin_Patient");
        String filePath = createStorageFilePath(split, batchFile);
        LOG.debug("Checking if patient deletes were received in " + filePath);

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
        CSVParser csvParser = new CSVParser(reader, EmisBatchSplitter.CSV_FORMAT.withHeader());
        Iterator<CSVRecord> iterator = csvParser.iterator();

        int countRecords = 0;
        int countDeletes = 0;

        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();
            countRecords ++;

            boolean deleted = Boolean.parseBoolean(record.get("Deleted"));
            if (deleted) {
                countDeletes ++;
            }
        }

        csvParser.close();

        if (countRecords == 0 && countDeletes == 0) {
            //in the new style, we receive an empty patient file
            return false;

        } else if (countRecords == countDeletes) {
            //in the old style, we received a file of 100% deletes
            return true;

        } else {
            throw new Exception("Failed to work out whether new or old pattern for disabled/restored Emis extract");
        }
    }

    /**
     * from 23/03/2019 onwards, when an Emis service becomes disabled, we no longer receive a "delete" for all patients
     * and when the fix is fixed, we no longer receive a re-bulk. This function supprts fixing that pattern.
     *
     * for the new style disabled/restored fix, we simply need to correct the sharing agreement file
     * since the data files are all empty and the feed simply resumes when its fixed
     */
    private void fixDisabledExtractNewWay() throws Exception {
        LOG.info("Fixing extract disabled new way");

        fixSharingAgreementFile();
        LOG.debug("Created new sharing agreement file(s)");

        copyNewFilesToStorage();
        LOG.debug("Copied to storage");
    }

    /**
     * prior to 23/03/2019 when Emis services became disabled, we received a "delete" for all patients, and
     * when the feed was fixed, we received a re-bulk of all data. This function supports fixing that pattern.
     */
    private void fixDisabledExtractOldWay() throws Exception {
        LOG.info("Fixing extract disabled old way");

        findOriginalBulk();

        //find out the GUIDs of any patient genuinely deleted or too old to be in the re-bulk
        findPatientsDeletedOrTooOldToBeInRebulk();

        Batch batchRebulked = batches.get(indexRebulked);
        for (BatchFile rebulkFile: batchRebulked.getBatchFiles()) {
            String fileType = rebulkFile.getFileTypeIdentifier();
            if (!isPatientFile(fileType)) {
                continue;
            }

            //create a file to replace the file on the disabled date
            fixFileType(fileType);

            //also create a version of the CSV file with just the header and nothing else in
            createEmptyFilesInBetweenDisableAndRebulk(fileType);
        }

        fixSharingAgreementFile();

        LOG.info("Written files to " + tempDir);
        dumpNewFileSizes(new File(tempDir));

        //copy everything into S3
        copyNewFilesToStorage();
        LOG.info("Copied files to permanent storage");
    }

    /**
     * once all the new files are created in our temp folder, we want to copy them to permanent storage
     */
    private void copyNewFilesToStorage() throws Exception {

        for (BatchSplit batchSplit: hmNewFilesCreated.keySet()) {
            List<BatchFile> batchFiles = hmNewFilesCreated.get(batchSplit);
            for (BatchFile batchFile: batchFiles) {

                String tempPath = createTempFilePath(batchSplit, batchFile, false);
                String storagePath = createStorageFilePath(batchSplit, batchFile);

                FileHelper.writeFileToSharedStorage(storagePath, new File(tempPath));
            }
        }

        //delete from temp
        FileUtils.deleteDirectory(new File(tempDir));
    }

    private static void dumpNewFileSizes(File f) {
        if (f.isDirectory()) {
            for (File child: f.listFiles()) {
                dumpNewFileSizes(child);
            }
        } else {
            String totalSizeReadable = FileUtils.byteCountToDisplaySize(f.length());
            LOG.info("" + f + " = " + totalSizeReadable);
        }
    }

    private void createTempDir() throws Exception {
        this.tempDir = instanceConfiguration.getTempDirectory();
        this.tempDir = FilenameUtils.concat(tempDir, "EmisDisabledFix");
        this.tempDir = FilenameUtils.concat(tempDir, org.getOdsCode());

        File f = new File(tempDir);
        if (f.exists()) {
            FileUtils.deleteDirectory(f);
        }

        f.mkdirs();
    }

    /**
     * we also need to copy the restored sharing agreement file to replace all the period it was disabled
     */
    private void fixSharingAgreementFile() throws Exception {

        //get the path of the agreement file from the re-bulk
        Batch batchRebulked = batches.get(indexRebulked);
        String rebulkedSharingAgreementFilePath = findSharingAgreementFilePath(batchRebulked);

        //then copy that file for each batch where the feed was disabled
        for (int i=indexDisabled; i<indexRebulked; i++) {
            Batch batch = batches.get(i);
            BatchFile batchFile = findSharingAgreementFile(batch);
            BatchSplit batchSplit = hmBatchSplits.get(batch);

            String newFile = createTempFilePath(batchSplit, batchFile, true);

            InputStream inputStream = FileHelper.readFileFromSharedStorage(rebulkedSharingAgreementFilePath);
            File replacementFileObj = new File(newFile);
            Files.copy(inputStream, replacementFileObj.toPath());
            inputStream.close();
        }
    }

    /**
     * the re-bulk won't contain any data for patient previously deleted or who was deducted/deceased
     * more than a year ago. So check all the admin_patient files received to find these patient GUIDs
     */
    private void findPatientsDeletedOrTooOldToBeInRebulk() throws Exception {

        patientGuidsDeletedOrTooOld = new HashSet<>();

        for (Batch batch: batches) {

            //skip any batches where we've been told to delete everyone
            if (isDisabledInSharingAgreementFile(batch)) {
                continue;
            }

            BatchSplit batchSplit = hmBatchSplits.get(batch);
            BatchFile batchFile = findBatchFile(batch, "Admin_Patient");
            String filePath = createStorageFilePath(batchSplit, batchFile);

            InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
            CSVParser csvParser = new CSVParser(reader, EmisBatchSplitter.CSV_FORMAT.withHeader());
            Iterator<CSVRecord> iterator = csvParser.iterator();

            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                String patientGuid = record.get("PatientGuid");

                //if this patient record has been deleted, then we won't expect it in the re-bulk and that's fine
                boolean deleted = Boolean.parseBoolean(record.get("Deleted"));

                //if this patient is deceased or deducted, then we won't expect the re-bulk to have it
                String dateOfDeath = record.get("DateOfDeath");
                String dateOfDeduction = record.get("DateOfDeactivation");

                if (!Strings.isNullOrEmpty(dateOfDeath)
                        || !Strings.isNullOrEmpty(dateOfDeduction)
                        || deleted) {
                    this.patientGuidsDeletedOrTooOld.add(patientGuid);
                }
            }

            csvParser.close();
        }
    }

    private Set<StringMemorySaver> findGuidsInRebulk(String fileType) throws Exception {

        String guidColumnName = getGuidColumnName(fileType);

        //find all the guids in the re-bulk
        Set<StringMemorySaver> idsInRebulk = new HashSet<>();

        Batch batchRebulked = batches.get(indexRebulked);
        BatchSplit batchSplitRebulked = hmBatchSplits.get(batchRebulked);
        BatchFile batchFileRebulked = findBatchFile(batchRebulked, fileType);
        String rebulkFilePath = createStorageFilePath(batchSplitRebulked, batchFileRebulked);
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(rebulkFilePath);
        CSVParser csvParser = new CSVParser(reader, EmisBatchSplitter.CSV_FORMAT.withHeader());

        try {
            //we'll need the headers for later, so get them now, while we've got the file open
            String[] headers = getHeaderMapAsArray(csvParser);
            hmFileHeadersByType.put(fileType, headers);

            Iterator<CSVRecord> iterator = csvParser.iterator();

            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();
                String id = record.get(guidColumnName);
                idsInRebulk.add(new StringMemorySaver(id));
            }
        } finally {
            csvParser.close();
        }

        LOG.info("Found " + idsInRebulk.size() + " IDs in re-bulk file: " + rebulkFilePath);

        return idsInRebulk;
    }

    private void fixFileType(String fileType) throws Exception {
        LOG.info("Doing " + fileType);

        //find all the guids in the re-bulk so we know what we've just been sent
        Set<StringMemorySaver> idsInRebulk = findGuidsInRebulk(fileType);
        //LOG.trace("Found " + idsInRebulk.size() + " IDs in re-bulk file");

        //create a replacement file for the exchange the service was disabled
        Batch batchDisabled = batches.get(indexDisabled);
        BatchSplit batchSplitDisabled = hmBatchSplits.get(batchDisabled);
        BatchFile batchFileDisabled = findBatchFile(batchDisabled, fileType);

        String replacementDisabledFile = createTempFilePath(batchSplitDisabled, batchFileDisabled, true);
        LOG.info("Created replacement file " + replacementDisabledFile);

        String[] headers = hmFileHeadersByType.get(fileType);

        //open the CSV printer to the file
        FileWriter fileWriter = new FileWriter(replacementDisabledFile);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, EmisBatchSplitter.CSV_FORMAT.withHeader(headers));
        csvPrinter.flush();

        Set<StringMemorySaver> pastIdsProcessed = new HashSet<>();

        String guidColumnName = getGuidColumnName(fileType);

        //now go through all files of the same type PRIOR to the service was disabled
        //to find any rows that we'll need to explicitly delete because they were deleted while
        //the extract was disabled
        for (int i=indexDisabled-1; i>=indexOriginallyBulked; i--) {
            Batch batch = batches.get(i);
            BatchSplit batchSplit = hmBatchSplits.get(batch);
            BatchFile batchFile = findBatchFile(batch, fileType);

            String originalFile = createStorageFilePath(batchSplit, batchFile);

            LOG.info("    Reading " + originalFile);
            InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(originalFile);
            CSVParser csvParser = new CSVParser(reader, EmisBatchSplitter.CSV_FORMAT.withHeader());
            try {
                Iterator<CSVRecord> iterator = csvParser.iterator();

                while (iterator.hasNext()) {
                    CSVRecord record = iterator.next();
                    String patientGuid = record.get("PatientGuid");
                    String recordGuid = record.get(guidColumnName);
                    StringMemorySaver recordGuidSaver = new StringMemorySaver(recordGuid);

                    //if the re-bulk contains a record matching this one, then it's OK
                    if (idsInRebulk.contains(recordGuidSaver)) {
                        continue;
                    }

                    //if we're already handled this record in a more recent extract, then skip it
                    if (pastIdsProcessed.contains(recordGuidSaver)) {
                        continue;
                    }
                    pastIdsProcessed.add(recordGuidSaver);
                    /*if (pastIdsProcessed.size() % 10000 == 0) {
                        LOG.trace("Got " + pastIdsProcessed.size() + " past IDs we've already processed");
                    }*/

                    //if the record is deleted, then we won't expect it in the re-bulk
                    boolean deleted = Boolean.parseBoolean(record.get("Deleted"));
                    if (deleted) {
                        continue;
                    }

                    //if it's not the patient file and we refer to a patient that we know
                    //has been deleted, then skip this row, since we know we're deleting the entire patient record
                    if (patientGuidsDeletedOrTooOld.contains(patientGuid)) {
                        continue;
                    }

                    //if this ID isn't deleted and isn't in the re-bulk then it means
                    //it WAS deleted in Emis Web but we didn't receive the delete, because it was deleted
                    //from Emis Web while the extract feed was disabled
                    //so create a new CSV record, carrying over the GUIDs from the original but marking as deleted
                    String[] newRecord = new String[headers.length];

                    for (int j=0; j<newRecord.length; j++) {
                        String header = headers[j];

                        //carry over these guid columns, as that's what normally happens for record deletes
                        if (header.equals("PatientGuid")
                                || header.equals("OrganisationGuid")
                                || header.equals(guidColumnName)) {

                            String val = record.get(header);
                            newRecord[j] = val;

                        } else if (header.equals("Deleted")) {
                            newRecord[j] = "true";

                        } else {
                            newRecord[j] = "";
                        }
                    }

                    //write the new CSV record out
                    csvPrinter.printRecord((Object[])newRecord);
                    csvPrinter.flush();

                    //log out the raw record that's missing from the original
                    StringBuilder sb = new StringBuilder();
                    sb.append("Record not in re-bulk: ");
                    for (int j=0; j<record.size(); j++) {
                        if (j > 0) {
                            sb.append(",");
                        }
                        sb.append(record.get(j));
                    }
                    LOG.info(sb.toString());
                }
            } finally {
                csvParser.close();
            }
        }

        csvPrinter.flush();
        csvPrinter.close();
    }

    /**
     * we've created a replacement file for the date the extract was disabled, but we need to
     * create an empty file for any dates in between these
     */
    private void createEmptyFilesInBetweenDisableAndRebulk(String fileType) throws Exception {

        for (int i=indexDisabled+1; i<indexRebulked; i++) {
            Batch batch = batches.get(i);
            BatchFile batchFile = findBatchFile(batch, fileType);
            BatchSplit batchSplit = hmBatchSplits.get(batch);

            String emptyTempFile = createTempFilePath(batchSplit, batchFile, true);

            String[] headers = hmFileHeadersByType.get(fileType);

            FileWriter fileWriter = new FileWriter(emptyTempFile);
            BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
            CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, EmisBatchSplitter.CSV_FORMAT.withHeader(headers));
            csvPrinter.flush();
            csvPrinter.close();

            LOG.info("Created empty file " + emptyTempFile);
        }

    }

    private static String[] getHeaderMapAsArray(CSVParser parser) {
        Map<String, Integer> headerMap = parser.getHeaderMap();
        if (headerMap == null) {
            throw new RuntimeException("Null header map returned from CSV file - ensure that .withHeader() is specified on the CSVFormat used");
        }

        String[] ret = new String[headerMap.size()];

        for (String col: headerMap.keySet()) {
            Integer colIndex = headerMap.get(col);
            ret[colIndex.intValue()] = col;
        }

        return ret;
    }

    private void findOriginalBulk() throws Exception {

        //go back from when disabled to find the previous bulk load (i.e. the first one or one after it was previously not disabled)
        for (int i=indexDisabled-1; i>=0; i--) {
            Batch batch = batches.get(i);

            boolean disabled = isDisabledInSharingAgreementFile(batch);
            if (disabled) {
                break;
            }

            indexOriginallyBulked = i;
        }

        if (indexOriginallyBulked > -1) {
            Batch batchOriginallyBulked = batches.get(indexOriginallyBulked);
            LOG.info("Originally bulked on " + batchOriginallyBulked.getBatchIdentifier() + " batch ID " + batchOriginallyBulked.getBatchId());
        }

        if (indexOriginallyBulked == -1) {
            throw new Exception("Failed to find exchanges for original bulk (" + indexOriginallyBulked + ")");
        }
    }

    private void findDisableAndRebulk() throws Exception {

        for (int i=batches.size()-1; i>=0; i--) {
            Batch batch = batches.get(i);

            boolean disabled = isDisabledInSharingAgreementFile(batch);

            if (disabled) {
                indexDisabled = i;

            } else {
                if (indexDisabled == -1) {
                    indexRebulked = i;
                } else {
                    //if we've found a non-disabled extract older than the disabled ones,
                    //then we've gone far enough back
                    break;
                }
            }
        }

        if (indexDisabled > -1) {
            Batch batchDisabled = batches.get(indexDisabled);
            LOG.info("Disabled on " + batchDisabled.getBatchIdentifier() + " batch ID " + batchDisabled.getBatchId());
        }

        if (indexRebulked > -1) {
            Batch batchRebulked = batches.get(indexRebulked);
            LOG.info("Rebulked on " + batchRebulked.getBatchIdentifier() + " batch ID " + batchRebulked.getBatchId());
        }

        if (indexDisabled == -1
                || indexRebulked == -1) {
            throw new Exception("Failed to find exchanges for disabling (" + indexDisabled + ") or re-bulking (" + indexRebulked + ")");
        }
    }

    private void retrieveBatches() throws Exception {
        String configurationId = dbConfiguration.getConfigurationId();
        this.batches = db.getAllBatches(configurationId);
        //LOG.debug("Found " + batches.size() + " for configuration " + configurationId);

        //ensure batches are sorted properly
        batches.sort((o1, o2) -> {
            Integer i1 = o1.getSequenceNumber();
            Integer i2 = o2.getSequenceNumber();
            return i1.compareTo(i2);
        });

        this.hmBatchSplits = new HashMap<>();

        //remove any batches from before our org was added to the extract
        String odsCode = org.getOdsCode();

        //iterate backwards, so we can remove as we go
        for (int i=batches.size()-1; i>=0; i--) {
            Batch batch = batches.get(i);

            BatchSplit splitForOrg = null;

            List<BatchSplit> splits = db.getBatchSplitsForBatch(batch.getBatchId());
            for (BatchSplit split: splits) {
                if (split.getOrganisationId().equalsIgnoreCase(odsCode)) {
                    splitForOrg = split;
                    break;
                }
            }

            if (splitForOrg == null) {
                //if no split for this org, remove from the list of batches
                //LOG.debug("No split found for batch " + batch.getBatchId());
                batches.remove(i);
            } else {
                hmBatchSplits.put(batch, splitForOrg);
                //LOG.debug("Split found for batch " + batch.getBatchId());
            }
        }

        LOG.info("Found " + batches.size() + " batches");
    }

    private BatchFile findSharingAgreementFile(Batch batch) throws Exception {
        return findBatchFile(batch, EmisHelper.EMIS_AGREEMENTS_FILE_ID);
    }

    private BatchFile findBatchFile(Batch batch, String fileTypeToFind) throws Exception {

        for (BatchFile file: batch.getBatchFiles()) {
            String fileType = file.getFileTypeIdentifier();
            if (fileType.equals(fileTypeToFind)) {
                return file;
            }
        }

        throw new Exception("Failed to find " + fileTypeToFind + " file in batch " + batch.getBatchId());
    }

    private String findSharingAgreementFilePath(Batch batch) throws Exception {

        BatchFile file = findSharingAgreementFile(batch);
        BatchSplit split = hmBatchSplits.get(batch);
        return createStorageFilePath(split, file);
    }

    private String createStorageFilePath(BatchSplit split, BatchFile file) {

        String filePath = this.instanceConfiguration.getSharedStoragePath();
        filePath = FilenameUtils.concat(filePath, dbConfiguration.getLocalRootPath());
        filePath = FilenameUtils.concat(filePath, split.getLocalRelativePath());

        //the batch file name has the GPG extension, so remove that to get the CSV file name
        String name = file.getFilename();
        name = FilenameUtils.removeExtension(name);
        filePath = FilenameUtils.concat(filePath, name);

        return filePath;
    }

    private String createTempFilePath(BatchSplit split, BatchFile file, boolean addToMap) {

        String filePath = this.tempDir;
        filePath = FilenameUtils.concat(filePath, dbConfiguration.getLocalRootPath());
        filePath = FilenameUtils.concat(filePath, split.getLocalRelativePath());

        //create the dir if it's not already there
        File f = new File(filePath);
        if (!f.exists()) {
            f.mkdirs();
        }

        //the batch file name has the GPG extension, so remove that to get the CSV file name
        String name = file.getFilename();
        name = FilenameUtils.removeExtension(name);
        filePath = FilenameUtils.concat(filePath, name);

        //and stick in our map so we know to copy it to S3 later
        if (addToMap) {
            List<BatchFile> l = hmNewFilesCreated.get(split);
            if (l == null) {
                l = new ArrayList<>();
                hmNewFilesCreated.put(split, l);
            }
            l.add(file);
        }

        return filePath;
    }



    private boolean isDisabledInSharingAgreementFile(Batch batch) throws Exception {
        String file = findSharingAgreementFilePath(batch);

        InputStreamReader reader = null;
        try {
            reader = FileHelper.readFileReaderFromSharedStorage(file);
        } catch (Exception ex) {
            throw new Exception("Failed to read " + file, ex);
        }

        CSVFormat format = EmisBatchSplitter.CSV_FORMAT.withHeader();
        CSVParser csvParser = new CSVParser(reader, format);
        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            CSVRecord record = iterator.next();

            String s = record.get("Disabled");
            boolean disabled = Boolean.parseBoolean(s);
            return disabled;

        } finally {
            csvParser.close();
        }
    }

    private static boolean isPatientFile(String fileType) {
        if (fileType.equals("Admin_Patient")
                || fileType.equals("CareRecord_Consultation")
                || fileType.equals("CareRecord_Diary")
                || fileType.equals("CareRecord_Observation")
                || fileType.equals("CareRecord_Problem")
                || fileType.equals("Prescribing_DrugRecord")
                || fileType.equals("Prescribing_IssueRecord")) {
            //note the referral file doesn't have a Deleted column, so isn't in this list

            return true;

        } else {
            return false;
        }
    }


    private static String getGuidColumnName(String fileType) {
        if (fileType.equals("Admin_Patient")) {
            return "PatientGuid";

        } else if (fileType.equals("CareRecord_Consultation")) {
            return "ConsultationGuid";

        } else if (fileType.equals("CareRecord_Diary")) {
            return "DiaryGuid";

        } else if (fileType.equals("CareRecord_Observation")) {
            return "ObservationGuid";

        } else if (fileType.equals("CareRecord_Problem")) {
            //there is no separate problem GUID, as it's just a modified observation
            return "ObservationGuid";

        } else if (fileType.equals("Prescribing_DrugRecord")) {
            return "DrugRecordGuid";

        } else if (fileType.equals("Prescribing_IssueRecord")) {
            return "IssueRecordGuid";

        } else {
            throw new IllegalArgumentException(fileType);
        }
    }

}
