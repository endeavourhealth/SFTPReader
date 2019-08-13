package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class TppBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(TppBatchSplitter.class);

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);
    private static final String SPLIT_COLUMN_ORG = "IDOrganisationVisibleTo";
    private static final String SPLIT_FOLDER = "Split";
    private static final String ORGANISATION_FILE = "SROrganisation.csv";
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

            //copy everything to storage
            String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
            storagePath = FilenameUtils.concat(storagePath, orgId);

            File[] splitFiles = orgDir.listFiles();
            LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage");

            for (File splitFile : splitFiles) {

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }

        return ret;
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
