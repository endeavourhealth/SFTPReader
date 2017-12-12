package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.PgpUtil;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;

public class EmisSftpBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(EmisSftpBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayer db) throws Exception {

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

            //if this one has already been decrypted, skip it
            //we delete the decrypted file after splitting, so it we're back in this function, we need to decrypt it again
            /*if (batchFile.isDecrypted()) {
                LOG.info("" + encryptedFilename + " has already been decrypted");
                continue;
            }*/

            String extension = dbConfiguration.getPgpFileExtensionFilter();
            String decryptedFilename = StringUtils.removeEnd(encryptedFilename, extension);

            String encryptedSourceFile = FilenameUtils.concat(storageDir, encryptedFilename);
            String decryptedTempFile = FilenameUtils.concat(tempDir, decryptedFilename);

            String privateKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKey();
            String privateKeyPassword = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();
            String publicKey = dbConfiguration.getPgpConfiguration().getPgpSenderPublicKey();

            LOG.info("   Decrypting file to: " + decryptedTempFile);
            InputStream inputStream = FileHelper.readFileFromSharedStorage(encryptedSourceFile);

            try {
                PgpUtil.decryptAndVerify(inputStream, decryptedTempFile, privateKey, privateKeyPassword, publicKey);

                long decryptedFileSize = new File(decryptedTempFile).length();

                batchFile.setDecryptedFilename(decryptedFilename);
                batchFile.setDecryptedSizeBytes(decryptedFileSize);

                db.setFileAsDecrypted(batchFile);

            } finally {
                inputStream.close();
            }

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
