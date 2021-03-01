package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppDataControllerFilterHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppRebulkFilterHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class TppBatchSplitter extends SftpBatchSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(TppBatchSplitter.class);

    private static final String SPLIT_COLUMN_ORG = "IDOrganisationVisibleTo";

    private static final String SPLIT_FOLDER = "Split";

    private static Set<String> cachedFilesToNotSplit = null;
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    /**
     * splits the TPP extract files org ID, storing the results in sub-directories using that org ID as the name
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db,
                                       DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

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

        File dstDir = new File(splitTempDir);

        LOG.trace("Splitting CSV files to " + splitTempDir);

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
            splitFile(f.getAbsolutePath(), dstDir, TppConstants.CSV_FORMAT.withHeader(), SPLIT_COLUMN_ORG);
        }

        //the splitting will have created a directory for each organisation in the files, so use
        //the directory listing to tell us what orgs there are
        List<File> orgDirs = new ArrayList<>();
        for (File orgDir : dstDir.listFiles()) {
            orgDirs.add(orgDir);
        }

        //create any org dirs for services we normally expect in the extract but haven't got today
        createMissingOrgDirs(orgDirs, filesToNotSplit, sourceTempDir, splitTempDir, dbConfiguration.getConfigurationId(), db);

        //copy the non-splitting files into each of the org directories
        for (File f : filesToNotSplit) {
            for (File orgDir : orgDirs) {

                File dst = new File(orgDir, f.getName());
                Files.copy(f.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        //we need to parse the organisation file, to store the mappings for later
        LOG.trace("Saving TPP organisation map");
        saveAllOdsCodes(sourceTempDir, db);
        LOG.trace("Completed CSV file splitting to " + dstDir);

        List<BatchSplit> batchSplits = new ArrayList<>();

        //for each org dir created we now need to do some filtering and copy the files into S3
        for (File orgDir : orgDirs) {

            int batchId = batch.getBatchId();
            String orgId = orgDir.getName();
            String localPath = FileHelper.concatFilePath(batch.getLocalRelativePath(), SPLIT_FOLDER, orgId);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batchId);
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(orgId); //the TPP org ID IS the ODS Code
            batchSplits.add(batchSplit);

            //remove any shared community data
            TppDataControllerFilterHelper.filterFilesForSharedData(orgDir, db);

            //remove any duplicate SRCode data
            filterFilesForDuplicateData(orgDir, batch, batchSplit, db, instanceConfiguration, dbConfiguration);

            //copy all files from temp dir to S3
            copyToPermanentStorage(orgDir, sourcePermDir);
        }

        return batchSplits;
    }

    /**
     * filters out records from the SRCode file that have been received before, so that when TPP send
     * re-bulks of SRCode files (which has happened about five times in the first half of 2020), we don't end up
     * having to process 100s of GB of SRCode file which is duplicated.
     * https://endeavourhealth.atlassian.net/browse/SD-70
     */
    private void filterFilesForDuplicateData(File orgDir, Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String orgId = orgDir.getName();
        File[] splitFiles = orgDir.listFiles();

        LOG.trace("Filtering duplicate data out of " + splitFiles.length + " files in " + orgDir);
        for (File splitFile : splitFiles) {
            String fileName = splitFile.getName();
            if (fileName.equalsIgnoreCase(TppConstants.CODE_FILE)) {

                String uniqueKey = TppConstants.COL_ROW_IDENTIFIER_TPP;

                //we only want to filter the file if we've got a re-bulk of SRCode and not anything else. If we genuinely
                //have a full re-bulk then we still want to update the hash table, but don't want to filter the file.
                boolean isBulk = TppBulkDetector.isBulkExtractNoAlert(batch, batchSplit, db, instanceConfiguration, dbConfiguration);
                boolean actuallyFilterFile = !isBulk; //don't actually filter the file if the extract is a bulk

                //tell us this is happening
                if (actuallyFilterFile) {

                    //we call this fn for every delta, so check the manifest to see if SRCode is telling us it's being re-bulked
                    boolean srCodeRebulk = false;
                    File manifestFile = new File(orgDir, TppConstants.MANIFEST_FILE);
                    List<ManifestRecord> records = ManifestRecord.readManifestFile(manifestFile);
                    for (ManifestRecord record: records) {
                        if (record.getFileNameWithExtension().equalsIgnoreCase(fileName)) {
                            srCodeRebulk = !record.isDelta();
                        }
                    }

                    if (srCodeRebulk) {
                        String fileSizeDesc = FileUtils.byteCountToDisplaySize(splitFile.length());
                        String msg = "TPP Re-bulk of SRCode detected in " + dbConfiguration.getConfigurationId() + " at " + fileSizeDesc;
                        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);
                    }
                }

                String filePath = splitFile.getAbsolutePath();
                LocalDateTime dt = TppFilenameParser.parseBatchIdentifier(batch.getBatchIdentifier());

                TppRebulkFilterHelper.filterFileForDuplicateData(orgId, filePath, uniqueKey, dt, actuallyFilterFile, instanceConfiguration);
            }
        }

    }


    private static void copyToPermanentStorage(File orgDir, String sourcePermDir) throws Exception {

        String orgId = orgDir.getName();
        File[] splitFiles = orgDir.listFiles();

        String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
        storagePath = FilenameUtils.concat(storagePath, orgId);

        //copy everything to storage
        LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage");
        for (File splitFile : splitFiles) {
            String fileName = splitFile.getName();
            String storageFilePath = FilenameUtils.concat(storagePath, fileName);
            FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
        }
    }

    /**
     * Before we copy the non-split files into the org directories, we need to make sure
     * to account for any organisations that ARE normally extracted but just so happen to have no data in this batch
     * by making sure we have an org directory for every org we had in our previous batch
     * <p/>
     * But ONLY do this if the non-split files are deltas. When a new service is added to a TPP feed, we get a
     * separate zip with the bulk data for that service, separate to the regular deltas, so don't copy
     * the non-split files from that bulk over for all the other services happily receiving deltas
     */
    private void createMissingOrgDirs(List<File> orgDirs, List<File> filesToNotSplit, String sourceTempDir,
                                      String splitTempDir, String configurationId, DataLayerI db) throws Exception {

        //if the non-patient files in this extract are bulks then we shouldn't apply to any
        //service not actually in this extract
        if (!areNonSplitFilesDeltas(sourceTempDir, filesToNotSplit)) {
            return;
        }

        //because the above check means that we don't always create org directories (and thus batchSplits) for all
        //orgs in our feed, we can't use the lastCompleteBatch to find the full set of orgs. So we need to get multiple
        //batches and use them all to find the distinct orgs our extract feed has
        List<Batch> batches = db.getAllBatches(configurationId); //ideally want just last X but this is fast enough
        batches.sort((o1, o2) -> {
            Integer i1 = o1.getSequenceNumber();
            Integer i2 = o2.getSequenceNumber();
            return i1.compareTo(i2);
        });

        int toCheck = 30; //limit to checking the last 30 batches

        for (int i = batches.size() - 1; i >= 0; i--) {
            Batch b = batches.get(i);

            List<BatchSplit> batchSplits = db.getBatchSplitsForBatch(b.getBatchId());
            for (BatchSplit batchSplit : batchSplits) {

                String orgId = batchSplit.getOrganisationId();
                String orgDir = FilenameUtils.concat(splitTempDir, orgId);
                File orgDirFile = new File(orgDir);

                if (!orgDirs.contains(orgDirFile)) {
                    FileHelper.createDirectoryIfNotExists(orgDir);
                    orgDirs.add(orgDirFile);
                }
            }

            toCheck--;
            if (toCheck <= 0) {
                break;
            }
        }

        /*List<BatchSplit> lastCompleteBatchSplits = db.getBatchSplitsForBatch(lastCompleteBatch.getBatchId());
        for (BatchSplit previousBatchSplit : lastCompleteBatchSplits) {

            String orgId = previousBatchSplit.getOrganisationId();
            String orgDir = FilenameUtils.concat(splitTempDir, orgId);
            File orgDirFile = new File(orgDir);

            if (!orgDirs.contains(orgDirFile)) {
                FileHelper.createDirectoryIfNotExists(orgDir);
                orgDirs.add(orgDirFile);
            }
        }*/

    }



    /**
     * checks the SRManifest file to work out if the non-patient files (i.e. those that don't get split
     * by organisation) are deltas or bulks. If there's a mix it will return true.
     */
    private boolean areNonSplitFilesDeltas(String sourceTempDir, List<File> filesToNotSplit) throws Exception {

        //first, read in the SRManifest file and find which files are deltas
        Map<String, Boolean> hmManifestContents = new HashMap<>();

        String orgFilePath = FilenameUtils.concat(sourceTempDir, TppConstants.MANIFEST_FILE);
        File f = new File(orgFilePath);
        if (!f.exists()) {
            throw new Exception("Failed to find " + TppConstants.MANIFEST_FILE + " in " + sourceTempDir);
        }

        List<ManifestRecord> records = ManifestRecord.readManifestFile(f);
        for (ManifestRecord record : records) {
            hmManifestContents.put(record.getFileNameWithExtension(), new Boolean(record.isDelta()));
        }

        //now check the files are all deltas
        boolean containsDelta = false;

        for (File fileToNotSplit : filesToNotSplit) {
            String fileName = fileToNotSplit.getName();

            //the Manifest file doesn't contain itself or the SRMapping files
            //and the Mapping file is processed into publisher_common so we don't need to worry about copying
            //that to every split directory
            if (fileName.equals(TppConstants.MANIFEST_FILE)
                    || fileName.equals(TppConstants.MAPPING_FILE)
                    || fileName.equals(TppConstants.MAPPING_GROUP_FILE)) {
                continue;
            }

            //the map doesn't contain file extensions
            Boolean isDelta = hmManifestContents.get(fileName);
            if (isDelta == null) {
                throw new Exception("Failed to find file " + fileToNotSplit + " in SRManifest in " + sourceTempDir);
            }

            if (isDelta.booleanValue()) {
                containsDelta = true;
                break;
            }
        }

        return containsDelta;
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
            /*if (getFilesToIgnore().contains(name)) {
                //ignore it

            } else*/

            if (getFilesToNotSplit().contains(name)) {
                filesToNotSplit.add(tempFile);

            } else {
                //if we're not sure, check for the presence of the column that we split by
                String firstChars = FileHelper.readFirstCharactersFromSharedStorage(tempFile.getAbsolutePath(), 10000);
                if (firstChars.contains(SPLIT_COLUMN_ORG)) {
                    //LOG.debug("Will split " + tempFile);
                    filesToSplit.add(tempFile);
                } else {
                    //LOG.debug("Will not split " + tempFile);
                    filesToNotSplit.add(tempFile);
                }
            }
        }
    }

    /*private static Set<String> getFilesToIgnore() {
        if (cachedFilesToIgnore == null) {
            Set<String> set = new HashSet<>();

            set.add("SRManifest");

            cachedFilesToIgnore = set;
        }
        return cachedFilesToIgnore;
    }*/

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
            set.add("SRManifest");

            cachedFilesToNotSplit = set;
        }
        return cachedFilesToNotSplit;
    }


    private static void saveAllOdsCodes(String sourceTempDir, DataLayerI db) throws Exception {

        String orgFilePath = FilenameUtils.concat(sourceTempDir, TppConstants.ORGANISATION_FILE);
        File f = new File(orgFilePath);

        //added to get around some issues when testing - this won't happen on Live
        if (!f.exists()) {
            LOG.warn(TppConstants.ORGANISATION_FILE + " not found in " + sourceTempDir);
            return;
        }
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis, Charset.forName(TppConstants.REQUIRED_CHARSET));
        CSVParser csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

        int done = 0;
        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            List<TppOrganisationMap> batch = new ArrayList<>();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                String orgName = csvRecord.get("Name");
                String orgOds = csvRecord.get("ID");

                if (!Strings.isNullOrEmpty(orgOds)) {
                    TppOrganisationMap mapping = new TppOrganisationMap();
                    mapping.setOdsCode(orgOds);
                    mapping.setName(orgName);

                    batch.add(mapping);

                    if (batch.size() >= 100) {
                        db.addTppOrganisationMappings(batch);
                        batch.clear();
                    }
                }

                done ++;
                if (done % 10000 == 0) {
                    LOG.debug("Saved " + done);
                }
            }

            if (!batch.isEmpty()) {
                db.addTppOrganisationMappings(batch);
            }

            LOG.debug("Finished saving " + done + " org records");

        } finally {
            csvParser.close();
        }
    }


    private static List<File> splitFile(String sourceFilePath, File dstDir, CSVFormat csvFormat, String... splitColmumns) throws Exception {
        CsvSplitter csvSplitter = new CsvSplitter(sourceFilePath, dstDir, false, csvFormat, TppConstants.getCharset(), splitColmumns);
        return csvSplitter.go();
    }
}
