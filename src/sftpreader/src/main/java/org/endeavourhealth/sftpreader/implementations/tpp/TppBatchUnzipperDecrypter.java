package org.endeavourhealth.sftpreader.implementations.tpp;

import net.lingala.zip4j.core.ZipFile;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class TppBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TppBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        //the main TPP extract file is in a multipart zip file, so we need to copy the parts to local disk then unzip
        String tempRootDir = instanceConfiguration.getTempDirectory();
        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();

        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        String tempDir = FilenameUtils.concat(tempRootDir, configurationDir);
        tempDir = FilenameUtils.concat(tempDir, batchDir);

        String storageDir = FilenameUtils.concat(sharedStoragePath, configurationDir);
        storageDir = FilenameUtils.concat(storageDir, batchDir);

        //ensure the temp dir exists
        FileHelper.createDirectoryIfNotExists(tempDir);

        List<String> tempFiles = new ArrayList<>();

        //copy all the files from storage to local disk
        for (BatchFile batchFile: batch.getBatchFiles()) {

            String filename = batchFile.getFilename();
            String sourceFile = FilenameUtils.concat(storageDir, filename);

            String tempFile = FilenameUtils.concat(tempDir, filename);
            tempFiles.add(tempFile);

            //just delete if it already exists
            FileHelper.deleteRecursiveIfExists(tempFile);

            InputStream inputStream = FileHelper.readFileFromSharedStorage(sourceFile);
            try {
                Files.copy(inputStream, new File(tempFile).toPath());

            } finally {
                inputStream.close();
            }
        }

        //now we've got everything local, go through an unzip anything that looks like a zip (there should just be the one)
        for (String tempFile: tempFiles) {
            String ext = FilenameUtils.getExtension(tempFile);
            if (!ext.equalsIgnoreCase("zip")) {
                continue;
            }

            //note that the zip4j library automatically handles multi-part zips provided they're in the same directory, which they are
            ZipFile zipFile = new ZipFile(tempFile);
            zipFile.extractAll(tempDir);
        }
    }
}
