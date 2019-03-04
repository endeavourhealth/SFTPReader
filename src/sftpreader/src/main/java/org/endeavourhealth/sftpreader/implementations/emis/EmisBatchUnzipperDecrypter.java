package org.endeavourhealth.sftpreader.implementations.emis;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.PgpUtil;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

public class EmisBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmisBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        //Emis files are PGP encrypted, so we need to decrypt them

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

            String encryptedFilename = batchFile.getFilename();

            String encryptedExtension = dbConfiguration.getPgpFileExtensionFilter();
            String decryptedFilename = StringUtils.removeEnd(encryptedFilename, encryptedExtension);

            String encryptedSourceFile = FilenameUtils.concat(storageDir, encryptedFilename);
            String decryptedTempFile = FilenameUtils.concat(tempDir, decryptedFilename);

            InputStream inputStream = FileHelper.readFileFromSharedStorage(encryptedSourceFile);

            //on some of the "transform" servers, we use already decrypted Emis data as the source, so
            //we don't have any encryption config. In this case, simply copy the source file into temp
            //so the outcome is the same as it being decrypted
            if (Strings.isNullOrEmpty(encryptedExtension)) {

                try {
                    LOG.info("   Copying w/o decryption to: " + decryptedTempFile);
                    Path destination = new File(decryptedTempFile).toPath();
                    Files.copy(inputStream, destination, StandardCopyOption.REPLACE_EXISTING);

                } finally {
                    inputStream.close();
                }

                continue;
            }

            //if this one has already been decrypted, skip it
            //we delete the decrypted file after splitting, so it we're back in this function, we need to decrypt it again
            /*if (batchFile.isDecrypted()) {
                LOG.info("" + encryptedFilename + " has already been decrypted");
                continue;
            }*/


            String privateKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKey();
            String privateKeyPassword = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();
            String publicKey = dbConfiguration.getPgpConfiguration().getPgpSenderPublicKey();

            try {
                LOG.info("   Decrypting file to: " + decryptedTempFile);
                PgpUtil.decryptAndVerify(inputStream, decryptedTempFile, privateKey, privateKeyPassword, publicKey);

                /*long decryptedFileSize = new File(decryptedTempFile).length();
                batchFile.setDecryptedFilename(decryptedFilename);
                batchFile.setDecryptedSizeBytes(decryptedFileSize);
                db.setFileAsDecrypted(batchFile);*/

            } finally {
                inputStream.close();
            }

            //also this is a good point to tag our GPG file so that our data retention policy thing works
            Map<String, String> tags = new HashMap<>();
            tags.put("Emis", "raw");
            FileHelper.setPermanentStorageTags(encryptedSourceFile, tags);

            //if we're using separate temp and permanent storage, then we want to move the decrypted file into permanent storage
            //taking out, since we store the split versions of the files in S3, there's no need to store the un-split versions too
            /*if (!FilenameUtils.equals(sharedStoragePath, tempRootDir)) {

                String decryptedPermanent = FilenameUtils.concat(sharedStoragePath, configurationDir);
                decryptedPermanent = FilenameUtils.concat(decryptedPermanent, batchDir);
                decryptedPermanent = FilenameUtils.concat(decryptedPermanent, decryptedFilename);

                File decryptedSource = new File(decryptedTempFile);
                FileHelper.writeFileToSharedStorage(decryptedPermanent, decryptedSource);
            }*/
        }
    }
}
