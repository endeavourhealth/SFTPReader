package org.endeavourhealth.sftpreader.implementations.homerton;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants.FILE_ID_PERSON;

public class HomertonBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonBatchSplitter.class);

    private static final String SPLIT_COLUMN_ORG = "source_description";
    private static final String PERSON_DOB_SPLIT_COLUMN_ORG = "birth_date_source_description";
    private static final String PERSON_GENDER_SPLIT_COLUMN_ORG = "gender_source_description";
    private static final String PERSON_ADDRESS_SPLIT_COLUMN_ORG = "address_source_description";
    private static final String SPLIT_FOLDER = "Split";
    private static Set<String> cachedFilesToNotSplit = null;

    /**
     * splits the Homerton extract files source_description, storing the results in sub-directories using that source_description as the name
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the big CSV files should already be in our temp storage. If so, use those files rather than the ones from permanent storage
        //String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        //sourceTempDir = FilenameUtils.concat(sourceTempDir, batchDir);

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
        identifyFiles(sourcePermDir, filesToSplit, filesToNotSplit);

        //split the files we can
        for (File f : filesToSplit) {

            String fileNameNoExt = FilenameUtils.getBaseName(f.getName());

            // the person files does not contain source_description so use the dob source description
            if (fileNameNoExt.endsWith(FILE_ID_PERSON)) {

                splitFile(f.getAbsolutePath(), dstDir, HomertonConstants.CSV_FORMAT, PERSON_DOB_SPLIT_COLUMN_ORG);
            } else {

                splitFile(f.getAbsolutePath(), dstDir, HomertonConstants.CSV_FORMAT, SPLIT_COLUMN_ORG);
            }
        }

        //the splitting will have created a directory for each organisation description in the files, so use
        //the directory listing to tell us what orgs there are
        List<File> orgDirs = new ArrayList<>();
        for (File orgDir : dstDir.listFiles()) {

            //if a directory has been created using the split column name then header data has been detected
            //in the files row data which is invalid, so simply ignore this directory and continue
            if (orgDir.getName().equalsIgnoreCase(SPLIT_COLUMN_ORG)
                    || orgDir.getName().equalsIgnoreCase(PERSON_DOB_SPLIT_COLUMN_ORG)) {
                continue;
            }

            //if a file (non directory) exists then the file cannot be split as the column data is blank.
            //log this event, ignore adding as a directory and continue
            if (!orgDir.isDirectory()) {
                LOG.warn("Data file: "+orgDir.getName()+" not fully split as it contains blank org references");
                continue;
            }

            //add a valid organisation directory to the list
            orgDirs.add(orgDir);
        }

        //create any org dirs for services we normally expect in the extract but haven't got today
        //createMissingOrgDirs(orgDirs, filesToNotSplit, sourceTempDir, splitTempDir, dbConfiguration.getConfigurationId(), db);

        //copy the non-splitting files into each of the org directories
        for (File f : filesToNotSplit) {
            for (File orgDir : orgDirs) {

                File dst = new File(orgDir, f.getName());
                Files.copy(f.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        LOG.trace("Completed CSV file splitting to " + dstDir);

        List<BatchSplit> batchSplits = new ArrayList<>();

        //for each org dir created we now need to do some filtering and copy the files into S3
        for (File orgDir : orgDirs) {

            int batchId = batch.getBatchId();
            String orgNameDesc = orgDir.getName();    //this is the org text description and path
            String localPath = FileHelper.concatFilePath(batch.getLocalRelativePath(), SPLIT_FOLDER, orgNameDesc);

            LOG.debug("local path for new org name ("+orgNameDesc+") split: "+localPath);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batchId);
            batchSplit.setLocalRelativePath(localPath);

            //set the ODS from a simple text lookup mapping
            String orgODS = HomertonHelper.getOdsFromOrgName(orgNameDesc);
            batchSplit.setOrganisationId(orgODS);
            batchSplits.add(batchSplit);

            //copy all files from temp dir to S3/local perm storage
            copyToPermanentStorage(orgDir, sourcePermDir);
        }

        return batchSplits;
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

    private void identifyFiles(String sourceTempDir, List<File> filesToSplit, List<File> filesToNotSplit) throws Exception {

        for (File tempFile : new File(sourceTempDir).listFiles()) {

            //the "Split" sub-directory will be there, so ignore it
            if (tempFile.isDirectory()) {
                continue;
            }

            String name = tempFile.getName();
            name = FilenameUtils.getBaseName(name);

            //NOTE - add in any files to not split, such as _deleted files with no source_description column
            if (getFilesToNotSplit().contains(name)) {

                filesToNotSplit.add(tempFile);
            } else {
                //if we're not sure, check for the presence of the column that we split by
                String firstChars = FileHelper.readFirstCharactersFromSharedStorage(tempFile.getAbsolutePath(), 5000);

                //the person file will contain the text source_description so will be added
                if (firstChars.contains(SPLIT_COLUMN_ORG)) {

                    filesToSplit.add(tempFile);
                } else {

                    filesToNotSplit.add(tempFile);
                }
            }
        }
    }

    private static Set<String> getFilesToNotSplit() {
        if (cachedFilesToNotSplit == null) {
            Set<String> set = new HashSet<>();

            //set.add("add in identified file names");

            cachedFilesToNotSplit = set;
        }
        return cachedFilesToNotSplit;
    }

    private static List<File> splitFile(String sourceFilePath, File dstDir, CSVFormat csvFormat, String... splitColumns) throws Exception {

        LOG.debug("Splitting: "+sourceFilePath);

        CsvSplitter csvSplitter = new CsvSplitter(sourceFilePath, dstDir, false, csvFormat, splitColumns);
        return csvSplitter.go();
    }
}
