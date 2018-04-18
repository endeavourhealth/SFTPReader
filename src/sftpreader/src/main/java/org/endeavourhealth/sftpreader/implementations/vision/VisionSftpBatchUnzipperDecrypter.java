package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.ZipUtil;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;

import static org.endeavourhealth.sftpreader.utilities.ZipUtil.unZipFile;

public class VisionSftpBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(VisionSftpBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        //Vision files are zips so need unzipping

        String tempRootDir = instanceConfiguration.getTempDirectory();
        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();

        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        String tempDir = FilenameUtils.concat(tempRootDir, configurationDir);
        tempDir = FilenameUtils.concat(tempDir, batchDir);

        String storageDir = FilenameUtils.concat(sharedStoragePath, configurationDir);
        storageDir = FilenameUtils.concat(storageDir, batchDir);

        //ensure the temp dir exists
        File f = new File(tempDir);
        if (!f.exists()) {
            f.mkdirs();
        }

        for (BatchFile batchFile: batch.getBatchFiles()) {

            String fileName = batchFile.getFilename();

            File zipFile = null;

            String storageFilePath = FilenameUtils.concat(storageDir, fileName);
            if (!FilenameUtils.equals(sharedStoragePath, tempDir)) {
                //if we have a different temp to shared storage path, then we need to copy the file
                //from our storage to temp for the unzipping (since the unzipping library can't work with S3)
                String tempFilePath = FilenameUtils.concat(tempDir, fileName);
                zipFile = new File(tempFilePath);

                InputStream inputStream = FileHelper.readFileFromSharedStorage(storageFilePath);
                try {
                    Files.copy(inputStream, zipFile.toPath());

                } finally {
                    inputStream.close();
                }

            } else {
                zipFile = new File(storageFilePath);
            }

            //if we had an error at some point, we may end up calling into here when the source zip file has
            //already been unzipped and deleted, so we need to check if the source file exists
            if (!zipFile.exists()) {
                continue;
            }

            //Check if a valid zip file and then if it contains valid file types
            if (!ZipUtil.validZipFile(zipFile, dbConfiguration)) {
                continue;
            }

            //Extract zip file and delete source zip (or multi-part zip)
            String unzipDir = zipFile.getParent();
            List<File> extractedFiles = unZipFile(zipFile, unzipDir, true);
            LOG.debug("There are " + extractedFiles.size() + " file(s) extracted to: " + unzipDir + " for zip file: " + fileName);

            //and if using different temp to shared storage, we need to copy the unzipped files to storage
            if (!FilenameUtils.equals(sharedStoragePath, tempDir)) {

                for (File unzippedFile: extractedFiles) {
                    String unzippedFileName = unzippedFile.getName();
                    String storagePath = FilenameUtils.concat(storageDir, unzippedFileName);

                    //the unzipped file doesn't have the path, so we need to create a properly qualified file
                    File unzippedFileWithPath = new File(unzipDir, unzippedFileName);

                    FileHelper.writeFileToSharedStorage(storagePath, unzippedFileWithPath);
                }
            }
        }
    }
}
