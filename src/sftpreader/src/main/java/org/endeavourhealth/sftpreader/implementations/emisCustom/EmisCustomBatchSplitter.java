package org.endeavourhealth.sftpreader.implementations.emisCustom;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.implementations.emis.EmisBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
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

        //see if we have separate temp and storage dirs
        String sourcePermDirToCopyTo = null;
        if (!FilenameUtils.equals(tempDir, sharedStorageDir)) {
            sourcePermDirToCopyTo = sourcePermDir;
        }

        List<BatchSplit> batchSplits = new ArrayList<>();

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

            LOG.trace("Splitting " + srcFile);

            if (srcFileObj.getName().equals(EmisCustomFilenameParser.FILE_NAME_REG_STATUS)) {
                splitRegStatusFile(batch, srcFile, dstDir, sourcePermDirToCopyTo, db, batchSplits);

            } else if (srcFileObj.getName().equals(EmisCustomFilenameParser.FILE_NAME_ORIGINAL_TERMS)) {
                splitOriginalTermsFile(batch, srcFile, dstDir, sourcePermDirToCopyTo, batchSplits);

            } else {
                throw new Exception("Unsupported file " + srcFileObj.getName());
            }
        }

        return batchSplits;
    }

    private void splitOriginalTermsFile(Batch batch, String srcFile, File dstDir, String sourcePermDirToCopyTo, List<BatchSplit> batchSplits) throws Exception {
        //the original terms file doesn't have an org GUID, so split by the ODS code
        CsvSplitter csvSplitter = new CsvSplitter(srcFile, dstDir, CSVFormat.TDF, "OrganisationOds");
        List<File> splitFiles = csvSplitter.go();

        for (File splitFile: splitFiles) {

            File odsDir = splitFile.getParentFile();
            String odsCode = odsDir.getName();
            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, odsCode);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(odsCode);

            batchSplits.add(batchSplit);

            //if we're using separate temp storage to our permanent storage, then copy to it
            if (!Strings.isNullOrEmpty(sourcePermDirToCopyTo)) {

                String storagePath = FilenameUtils.concat(sourcePermDirToCopyTo, SPLIT_FOLDER);
                storagePath = FilenameUtils.concat(storagePath, odsCode);

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }
    }


    private void splitRegStatusFile(Batch batch, String srcFile, File dstDir, String sourcePermDirToCopyTo, DataLayerI db, List<BatchSplit> batchSplits) throws Exception {

        //split the file by org GUID
        CsvSplitter csvSplitter = new CsvSplitter(srcFile, dstDir, CSVFormat.TDF, "OrganisationGuid");
        List<File> splitFiles = csvSplitter.go();

        for (File splitFile: splitFiles) {

            File orgDir = splitFile.getParentFile();
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

            batchSplits.add(batchSplit);

            //if we're using separate temp storage to our permanent storage, then copy to it
            if (!Strings.isNullOrEmpty(sourcePermDirToCopyTo)) {

                String storagePath = FilenameUtils.concat(sourcePermDirToCopyTo, SPLIT_FOLDER);
                storagePath = FilenameUtils.concat(storagePath, orgGuid);

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }
    }
}
