package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.*;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class TppBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(TppBatchSplitter.class);

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);
    private static final String SPLIT_COLUMN_ORG = "IDOrganisationVisibleTo";
    private static final String FILTER_ORG_REG_AT_COLUMN = "IDOrganisationRegisteredAt";
    private static final String ORG_COLUMN = "IDOrganisation";
    private static final String PATIENT_ID_COLUMN = "IDPatient";
    private static final String REMOVED_DATA_COLUMN = "RemovedData";
    private static final String SPLIT_FOLDER = "Split";
    private static final String ORGANISATION_FILE = "SROrganisation.csv";
    private static final String PATIENT_REGISTRATION_FILE = "SRPatientRegistration.csv";
    private static final String REQUIRED_CHARSET = "Cp1252";

    private static Set<String> cachedFilesToIgnore = null;
    private static Set<String> cachedFilesToNotSplit = null;

    /**
     * splits the TPP extract files org ID, storing the results in sub-directories using that org ID as the name
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the big CSV files should already be in our temp storage. If so, use those files rather than the ones from permanent storage
        String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        sourceTempDir = FilenameUtils.concat(sourceTempDir, batchDir);

        String sourcePermDir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        sourcePermDir = FilenameUtils.concat(sourcePermDir, batchDir);

        String splitTempDir = FilenameUtils.concat(tempDir, configurationDir);
        splitTempDir = FilenameUtils.concat(splitTempDir, batchDir);
        splitTempDir = FilenameUtils.concat(splitTempDir, SPLIT_FOLDER);

        LOG.trace("Splitting CSV files to " + splitTempDir);

        File dstDir = new File(splitTempDir);

        //if the folder does exist, delete all content within it, since if we're re-splitting a file
        //we want to make sure that all previous content is deleted
        FileHelper.deleteRecursiveIfExists(dstDir);
        FileHelper.createDirectoryIfNotExists(dstDir);

        //work out which files we want to split
        List<File> filesToSplit = new ArrayList<>();
        List<File> filesToNotSplit = new ArrayList<>();
        identifyFiles(sourceTempDir, filesToSplit, filesToNotSplit);

        //split the files we can
        for (File f : filesToSplit) {

            splitFile(f.getAbsolutePath(), dstDir, CSV_FORMAT.withHeader(), SPLIT_COLUMN_ORG);
        }

        List<File> orgDirs = new ArrayList<>();
        for (File orgDir : dstDir.listFiles()) {
            orgDirs.add(orgDir);
        }

        //before we copy the non-split files into the org directories, we need to make sure
        //to account for any organisations that ARE extracted but just so happen to have no data in this instance
        //by making sure we have an org directory for every org we had in our previous batch
        if (lastCompleteBatch != null) {
            List<BatchSplit> lastCompleteBatchSplits = db.getBatchSplitsForBatch(lastCompleteBatch.getBatchId());
            for (BatchSplit previousBatchSplit : lastCompleteBatchSplits) {
                String localRelativePath = previousBatchSplit.getLocalRelativePath();
                String orgId = new File(localRelativePath).getName();

                String orgDir = FilenameUtils.concat(splitTempDir, orgId);
                FileHelper.createDirectoryIfNotExists(orgDir);

                File orgDirFile = new File(orgDir);

                if (!orgDirs.contains(orgDirFile)) {
                    orgDirs.add(orgDirFile);
                }
            }
        }

        //copy the non-splitting files into each of the org directories
        for (File f : filesToNotSplit) {
            for (File orgDir : orgDirs) {

                File dst = new File(orgDir, f.getName());
                Files.copy(f.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        //we need to parse the organisation file, to store the mappings for later
        saveAllOdsCodes(sourceTempDir, db, instanceConfiguration, dbConfiguration, batch);

        LOG.trace("Completed CSV file splitting to " + dstDir);

        //copy all our files to permanent storage and create the batch split objects
        //build a list of the folders containing file sets, to return
        List<BatchSplit> ret = new ArrayList<>();

        for (File orgDir : orgDirs) {

            String orgId = orgDir.getName();
            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, orgId);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(orgId); //the TPP org ID IS the ODS Code

            ret.add(batchSplit);

            File[] splitFiles = orgDir.listFiles();

            //filter each split file to include organisation and GMS registration data only
            LOG.trace("Filtering " + splitFiles.length + " files in " + orgDir);
            filterFiles (orgId, splitFiles, db);

            //copy everything to storage
            LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage");
            String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
            storagePath = FilenameUtils.concat(storagePath, orgId);

            for (File splitFile : splitFiles) {

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }

        return ret;
    }

    private static void filterFiles (String orgId, File[] splitFiles, DataLayerI db) throws Exception  {

        //save/update the split SRPatientRegistrationFile GMS orgs to the db
        for (File splitFile : splitFiles) {
            if (splitFile.getName().equalsIgnoreCase(PATIENT_REGISTRATION_FILE)) {

                LOG.debug("Found "+PATIENT_REGISTRATION_FILE+" file to save into db");
                FileInputStream fis = new FileInputStream(splitFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                InputStreamReader reader = new InputStreamReader(bis, Charset.forName(REQUIRED_CHARSET));
                CSVParser csvParser = new CSVParser(reader, CSV_FORMAT.withHeader());

                int count = 0;

                try {
                    Iterator<CSVRecord> csvIterator = csvParser.iterator();

                    //create a cached hashmap of Patient Gms orgs
                    HashMap<Integer,List<String>> cachedPatientGmsOrgs = new HashMap<>();

                    while (csvIterator.hasNext()) {
                        CSVRecord csvRecord = csvIterator.next();
                        String registrationStatus = csvRecord.get("RegistrationStatus");

                        // NOTE: IDOrganisationId is used to get the correct Orgs from patient registration status file,
                        // not the IDOrganisationRegisteredAt column
                        String gmsOrganisationId = csvRecord.get(ORG_COLUMN);
                        String patientId = csvRecord.get(PATIENT_ID_COLUMN);

                        // save GMS registrations only. Using .contains as some status values contain both,
                        // i.e. GMS,Contraception for example
                        if (registrationStatus.contains("GMS")
                                && !Strings.isNullOrEmpty(gmsOrganisationId) && !Strings.isNullOrEmpty(patientId)) {

                            //don't bother added the same organisation as the one being processed as that is the default
                            //check when filtering so no need to add it to the DB
                            if (gmsOrganisationId.equals(orgId)) {
                                continue;
                            }

                            Integer patId = Integer.parseInt(patientId);

                            //check if already added for that patient and org so do not attempt another db write
                            List<String> gmsOrgs;
                            if (cachedPatientGmsOrgs.containsKey(patId)) {

                                gmsOrgs = cachedPatientGmsOrgs.get(patId);
                                if (gmsOrgs.contains(gmsOrganisationId)) {
                                    continue;
                                }
                            } else {
                                gmsOrgs = new ArrayList<>();
                            }

                            TppOrganisationGmsRegistrationMap map = new TppOrganisationGmsRegistrationMap();
                            map.setOrganisationId(orgId);
                            map.setPatientId(patId);
                            map.setGmsOrganisationId(gmsOrganisationId);
                            db.addTppOrganisationGmsRegistrationMap(map);

                            //cache the patient Gms Orgs
                            gmsOrgs.add(gmsOrganisationId);
                            cachedPatientGmsOrgs.put(patId, gmsOrgs);
                            count++;
                        }
                    }
                } finally {
                    csvParser.close();
                }

                //once we have found and processed the Patient Registration File we can break out this first part
                LOG.debug(count+" potentially new distinct Patient GMS organisations filed for OrganisationId: "+orgId);
                break;
            }
        }

        //get the latest Gms org db entries for this organisation and pop them into a hash map
        List<TppOrganisationGmsRegistrationMap> maps = db.getTppOrganisationGmsRegistrationMapsFromOrgId(orgId);

        HashMap<Integer,List<String>> filterPatientGmsOrgs = new HashMap<>();
        int count = 0;
        for (TppOrganisationGmsRegistrationMap map : maps) {
            String filterGmsOrgId = map.getGmsOrganisationId();
            Integer patientId = map.getPatientId();

            List<String> filterGmsOrgs;
            if (filterPatientGmsOrgs.containsKey(patientId)) {

                filterGmsOrgs = filterPatientGmsOrgs.get(patientId);
            } else {

                filterGmsOrgs = new ArrayList<>();
            }

            filterGmsOrgs.add(filterGmsOrgId);
            filterPatientGmsOrgs.put(patientId, filterGmsOrgs);
            count++;
        }

        LOG.debug(count+" Patient GMS organisation records found in DB for OrganisationId: "+orgId);

        // finally, filter each each split file
        for (File splitFile : splitFiles) {

            //create the csv parser input
            FileInputStream fis = new FileInputStream(splitFile);
            BufferedInputStream bis = new BufferedInputStream(fis);
            InputStreamReader reader = new InputStreamReader(bis, Charset.forName(REQUIRED_CHARSET));
            CSVParser csvParser = new CSVParser(reader, CSV_FORMAT.withHeader());

            CSVPrinter csvPrinter = null;

            try {

                Map<String, Integer> headerMap = csvParser.getHeaderMap();  //get the file header
                // convert the header map into an ordered String array, so we can check for the filter column
                // and then populate the column headers for the new CSV files later on
                String [] columnHeaders = new String[headerMap.size()];
                Iterator<String> headerIterator = headerMap.keySet().iterator();
                boolean hasfilterOrgColumn = false;
                boolean hasRemovedDataHeader = false;
                while (headerIterator.hasNext()) {
                    String headerName = headerIterator.next();
                    if (headerName.equalsIgnoreCase(FILTER_ORG_REG_AT_COLUMN))
                        hasfilterOrgColumn = true;

                    if (headerName.equalsIgnoreCase(REMOVED_DATA_COLUMN))
                        hasRemovedDataHeader = true;

                    int headerIndex = headerMap.get(headerName);
                    columnHeaders[headerIndex] = headerName;
                }

                //if the filter column header is not in this file, ignore and continue to next file
                if (!hasfilterOrgColumn) {
                    //LOG.debug("Filter column not found in file: "+splitFile.getName()+", skipping...");
                    csvParser.close();
                    continue;
                }

                //create a new temp file output from the splitFile to write the filtered records
                File splitFileTmp = new File (splitFile+".tmp");
                FileOutputStream fos = new FileOutputStream(splitFileTmp);
                OutputStreamWriter osw = new OutputStreamWriter(fos, Charset.forName(REQUIRED_CHARSET));
                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                csvPrinter = new CSVPrinter(bufferedWriter, CSV_FORMAT.withHeader(columnHeaders));

                //create a new list of records based on the filtering process
                LOG.debug("Filtering file: "+splitFile);

                Iterator<CSVRecord> csvIterator = csvParser.iterator();
                count = 0;

                while (csvIterator.hasNext()) {
                    CSVRecord csvRecord = csvIterator.next();

                    // if the filter org column value matches the receiving org, include the data by default
                    // OR if it contains a removed data header header and it is a delete == 1, include the data
                    // OR is contained in the GMS organisation list for that patient, include the data
                    if (csvRecord.get(FILTER_ORG_REG_AT_COLUMN).equals(orgId)) {

                        csvPrinter.printRecord(csvRecord);
                        count++;

                    } else if (hasRemovedDataHeader && csvRecord.get(REMOVED_DATA_COLUMN).equals("1")) {
                        //RemovedData == 1 items have no registered organisation, so write anyway

                        csvPrinter.printRecord(csvRecord);
                        count++;

                    } else {
                        //otherwise, perform filtering on patient and additional GMS registered orgs

                        String patientId = csvRecord.get(PATIENT_ID_COLUMN);
                        Integer patId = Integer.parseInt(patientId);
                        String organisationRegisteredAt = csvRecord.get(FILTER_ORG_REG_AT_COLUMN);
                        if (filterPatientGmsOrgs.containsKey(patId)) {

                            List<String> filterGmsOrgs = filterPatientGmsOrgs.get(patId);
                            if (filterGmsOrgs.contains(organisationRegisteredAt)) {

                                csvPrinter.printRecord(csvRecord);
                                count++;
                            } //else {

                                //LOG.debug("Record excluded from filter: " + csvRecord.toString());
                            //}
                        } //else {
                            //LOG.error("Record has no other Patient GMS organisations to filter with: "+csvRecord.toString());
                        //}
                    }
                }

                LOG.debug("File filtering done: "+count+" records written for file: "+splitFile);

                //delete original file
                splitFile.delete();

                //rename the filtered tmp file to original filename
                splitFileTmp.renameTo(splitFile);
            }
            finally {
                //make sure everything is closed
                if (csvParser != null) {
                    csvParser.close();
                    }
                if (csvPrinter != null) {
                    csvPrinter.close();
                }
            }
        }
    }

    private void identifyFiles(String sourceTempDir, List<File> filesToSplit, List<File> filesToNotSplit) throws Exception {
        for (File tempFile : new File(sourceTempDir).listFiles()) {

            //the "Split" sub-directory will be there, so ignore it
            if (tempFile.isDirectory()) {
                continue;
            }

            String name = tempFile.getName();

            //and we'll also have non-CSV files (the zip and fragments), so ignore them too
            String ext = FilenameUtils.getExtension(name);
            if (!ext.equalsIgnoreCase("csv")) {
                continue;
            }

            name = FilenameUtils.getBaseName(name);

            //NOTE - there are some files that DO contain an organisation ID column but shouldn't be split (e.g. SRCtv3Hierarchy),
            //so we need these explicit lists of how to handle each file, rather than being able to work it out dynamically
            if (getFilesToIgnore().contains(name)) {
                //ignore it

            } else if (getFilesToNotSplit().contains(name)) {
                filesToNotSplit.add(tempFile);

            } else {
                //if we're not sure, check for the presence of the column that we split by
                String firstChars = FileHelper.readFirstCharactersFromSharedStorage(tempFile.getAbsolutePath(), 10000);
                if (firstChars.contains(SPLIT_COLUMN_ORG)) {
                    LOG.debug("Will split " + tempFile);
                    filesToSplit.add(tempFile);
                } else {
                    LOG.debug("Will not split " + tempFile);
                    filesToNotSplit.add(tempFile);
                }
            }
        }
    }

    private static Set<String> getFilesToIgnore() {
        if (cachedFilesToIgnore == null) {
            Set<String> set = new HashSet<>();

            set.add("SRManifest");

            cachedFilesToIgnore = set;
        }
        return cachedFilesToIgnore;
    }

    private static Set<String> getFilesToNotSplit() {
        if (cachedFilesToNotSplit == null) {
            Set<String> set = new HashSet<>();

            set.add("SRCcg");
            set.add("SRCtv3");
            set.add("SRMapping");
            set.add("SRMappingGroup");
            set.add("SRConfiguredListOption");
            set.add("SRCtv3ToVersion2");
            set.add("SRCtv3ToSnomed");
            set.add("SRCtv3Hierarchy");
            set.add("SRImmunisationContent");
            set.add("SRMedicationReadCodeDetails");
            set.add("SROrganisation");
            set.add("SROrganisationBranch");
            set.add("SRStaff");
            set.add("SRStaffMemberProfile");
            set.add("SRStaffMember");
            set.add("SRStaffMemberProfileRole");
            set.add("SRTrust");

            //we don't transform these although we will retain them in the primary practice(s) file list
            set.add("SRQuestionnaire");
            set.add("SRTemplate");

            cachedFilesToNotSplit = set;
        }
        return cachedFilesToNotSplit;
    }


    private static void saveAllOdsCodes(String sourceTempDir, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws Exception {

        String orgFilePath = FilenameUtils.concat(sourceTempDir, ORGANISATION_FILE);
        File f = new File(orgFilePath);

        //added to get around some issues when testing - this won't happen on Live
        if (!f.exists()) {
            LOG.warn(ORGANISATION_FILE + " not found in " + sourceTempDir);
            return;
        }
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis, Charset.forName(REQUIRED_CHARSET));
        CSVParser csvParser = new CSVParser(reader, CSV_FORMAT.withHeader());

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                String orgName = csvRecord.get("Name");
                String orgOds = csvRecord.get("ID");

                if (!Strings.isNullOrEmpty(orgOds)) {
                    TppOrganisationMap mapping = new TppOrganisationMap();
                    mapping.setOdsCode(orgOds);
                    mapping.setName(orgName);

                    db.addTppOrganisationMap(mapping);
                }
            }
        } finally {
            csvParser.close();
        }
    }


    private static List<File> splitFile(String sourceFilePath, File dstDir, CSVFormat csvFormat, String... splitColmumns) throws Exception {
        CsvSplitter csvSplitter = new CsvSplitter(sourceFilePath, dstDir, false, csvFormat, Charset.forName(REQUIRED_CHARSET), splitColmumns);
        return csvSplitter.go();
    }
}
