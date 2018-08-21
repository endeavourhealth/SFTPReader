package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.implementations.emis.EmisBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class EmisCustomBatchSplitter extends SftpBatchSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomBatchSplitter.class);

    public static final String SPLIT_FOLDER = "Split";

    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the unzipped file should already be in our temp storage
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

        Set<File> orgIdDirs = new HashSet<>();

        for (File srcFileObj: new File(sourceTempDir).listFiles()) {

            if (srcFileObj.isDirectory()) {
                continue;
            }

            String srcFile = srcFileObj.getAbsolutePath();

            //the compressed 7z file will be in the same dir, so ignore that
            String extension = FilenameUtils.getExtension(srcFile);
            if (extension.equals("7z")) {
                continue;
            }

            //split the file by org GUID
            LOG.trace("Splitting " + srcFile);
            CsvSplitter csvSplitter = new CsvSplitter(srcFile, dstDir, CSVFormat.TDF, "OrganisationGuid");
            List<File> splitFiles = csvSplitter.go();
            for (File splitFile: splitFiles) {
                orgIdDirs.add(splitFile.getParentFile());
            }
        }

        //build a list of the folders containing file sets, to return
        List<BatchSplit> ret = new ArrayList<>();

        for (File orgDir : orgIdDirs) {

            String orgGuid = orgDir.getName();
            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, orgGuid);

            //look up the ODS code using the Emis org table, but adding the curly braces so the GUID
            //is in the same format as the regular extracts
            String odsCode = EmisBatchSplitter.findOdsCode("{" + orgGuid.toUpperCase() + "}", db);

            //we've received at least one set of data for a service we don't recognise
            if (odsCode == null) {
                LOG.error("Failed to find ODS code for EMIS Org GUID " + orgGuid + " so skipping that content");
                //throw new RuntimeException("Failed to find ODS code for EMIS Org GUID " + orgGuid);
                continue;
            }

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(odsCode);

            ret.add(batchSplit);

            //if we're using separate temp storage to our permanent storage, then copy everything to it
            if (!FilenameUtils.equals(tempDir, sharedStorageDir)) {

                String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
                storagePath = FilenameUtils.concat(storagePath, orgGuid);

                File[] splitFiles = orgDir.listFiles();
                LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage for " + odsCode);

                for (File splitFile: splitFiles) {

                    String fileName = splitFile.getName();
                    String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                    FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
                }
            }
        }

        return ret;
    }
}
