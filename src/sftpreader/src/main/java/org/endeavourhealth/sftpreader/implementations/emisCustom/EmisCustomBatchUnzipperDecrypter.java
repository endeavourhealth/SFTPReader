package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.PgpUtil;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;

public class EmisCustomBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmisCustomBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        //the Emis custom file is 7z compressed

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

        String password = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();

        for (BatchFile batchFile: batch.getBatchFiles()) {

            //7z decompression can't use a stream, so we need to copy the file to our local disk
            String filename = batchFile.getFilename();
            String sourceFile = FilenameUtils.concat(storageDir, filename);

            String tempFile = FilenameUtils.concat(tempDir, filename);

            //delete if it already exists
            FileHelper.deleteRecursiveIfExists(tempFile);

            InputStream inputStream = FileHelper.readFileFromSharedStorage(sourceFile);
            try {
                Files.copy(inputStream, new File(tempFile).toPath());
            } finally {
                inputStream.close();
            }

            //now we can decompress it
            SevenZFile sevenZFile = new SevenZFile(new File(tempFile), password.toCharArray());
            SevenZArchiveEntry entry = sevenZFile.getNextEntry();
            //long size = entry.getSize();
            String entryName = entry.getName();

            String unzippedFile = FilenameUtils.concat(tempDir, entryName);
            FileHelper.deleteRecursiveIfExists(unzippedFile);
            FileOutputStream fos = new FileOutputStream(unzippedFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);

            //the file Emis provide doesn't contain column headings, but later things are easier if we have
            //them, so just insert them first
            String headers = "OrganisationGuid\tPatientGuid\tDate\tRegistrationStatus\tRegistratationType\tProcessingOrder\r\n";
            bos.write(headers.getBytes());
            bos.flush();

            /*LOG.debug("entryName = " + entryName);
            LOG.debug("Size = " + size);
            LOG.debug("password = " + password);*/

            /*byte[] block = new byte[1024 * 1024 * 10]; //use 10MB buffer to decompress
            int offset = 0;*/

            while (true) {

                //can't get reading in blocks to work, so just do it byte by byte
                int b = sevenZFile.read();
                if (b == -1) {
                    break;
                }
                bos.write(b);

                /*int read = sevenZFile.read(block, offset, (int)size - offset);
                if (read == -1) {
                    break;
                }
                LOG.debug("Read " + read);

                bos.write(block, 0, read);
                offset += read;*/
            }

            //close everything
            bos.close();
            sevenZFile.close();
        }

    }
}
