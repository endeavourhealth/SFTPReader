package org.endeavourhealth.sftpreader.implementations.barts;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.SftpFile;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class BartsBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(BartsBatchValidator.class);

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        LocalDate batchDate = BartsFilenameParser.parseBatchIdentifier(incompleteBatch.getBatchIdentifier());

        //validate it's past 3pm if the batch is for today, because Barts drip files up over the morning. If before 3pm, just return false
        //so the batch silently fails validation and isn't processed any further
        LocalDate today = LocalDate.now();
        if (!batchDate.isBefore(today)) {

            //what we were told was just wrong. We get files dripped up to us all day, and although the SFTP Reader
            //handles files being received later than their batch date, it seems unnecessarily messy to have that
            //happening every day. So simply don't let a batch complete until the day later.
            return false;

            /*Calendar cal = GregorianCalendar.getInstance();
            cal.setTime(new Date());
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            if (hour < 15) {
                return false;
            }*/
        }

        try {
            // All CDS/SUS files must have a matching Tails file
            checkAllRequiredTailsFilesArePresentInBatch(db, instanceConfiguration, dbConfiguration, incompleteBatch);
            checkForSkippedSusFiles(incompleteBatch, lastCompleteBatch, dbConfiguration, db);

        } catch (Exception ex) {
            throw new SftpValidationException(ex);
        }


        //we were told that SUSOPA or TAILOPA should be the last file received, but we can receive
        //multiple of these in a day, so it's not possible to guarantee that the presence of these files
        //means we have ALL the files for the day. But the absence of the file does mean the batch is incomplete
        //But we only started receiving these files consistently from 15/12/2017, so only check for batches after this
        //NO, we don't get these consistently, so taking this check out
        /*LocalDate susOpaStartDate = BartsSftpFilenameParser.parseBatchIdentifier("2017-12-15");
        if (!batchDate.isBefore(susOpaStartDate)) {
            checkOutpatientFilesPresent(incompleteBatch);
        }
*/
        return true;
    }

    /**
     * SUS files are numbered sequentially - this fn checks to see if any numbers have been missed
     */
    private void checkForSkippedSusFiles(Batch incompleteBatch, Batch lastCompleteBatch, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        List<BatchFile> susInpatientFiles = new ArrayList<>();
        List<BatchFile> susOutpatientFiles = new ArrayList<>();
        List<BatchFile> susEmergencyFiles = new ArrayList<>();
        List<BatchFile> susEcdsFiles = new ArrayList<>();

        List<BatchFile> files = incompleteBatch.getBatchFiles();
        for (BatchFile file: files) {
            String fileType = file.getFileTypeIdentifier();

            //skip any file types that don't have a tail file
            if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSAEA)) {
                susEmergencyFiles.add(file);

            } else if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSOPA)) {
                susOutpatientFiles.add(file);

            } else if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSIP)) {
                susInpatientFiles.add(file);

            } else if (fileType.equals(BartsFilenameParser.TYPE_EMERGENCY_CARE)) {
                susEcdsFiles.add(file);

            } else {
                //not a sus file
            }
        }

        checkForSkippedSusTypeFiles(incompleteBatch, susInpatientFiles, lastCompleteBatch, dbConfiguration, db);
        checkForSkippedSusTypeFiles(incompleteBatch, susOutpatientFiles, lastCompleteBatch, dbConfiguration, db);
        checkForSkippedSusTypeFiles(incompleteBatch, susEmergencyFiles, lastCompleteBatch, dbConfiguration, db);
        checkForSkippedSusTypeFiles(incompleteBatch, susEcdsFiles, lastCompleteBatch, dbConfiguration, db);
    }

    private void checkForSkippedSusTypeFiles(Batch incompleteBatch, List<BatchFile> susFiles, Batch lastCompleteBatch, DbConfiguration dbConfiguration, DataLayerI db) throws Exception  {
        if (susFiles.isEmpty()) {
            return;
        }

        //find the previous max sequence number
        BatchFile f = susFiles.get(0);
        String fileType = f.getFileTypeIdentifier();
        LOG.trace("Received " + susFiles.size() + " " + fileType + " files");

        int sequenceNumber = findPreviousSequenceNumber(fileType, lastCompleteBatch, dbConfiguration, db);
        LOG.trace("Last sequence number = " + sequenceNumber);

        susFiles.sort((o1, o2) -> {
            int i1 = getFileNumber(o1.getFilename());
            int i2 = getFileNumber(o2.getFilename());
            return Integer.compare(i1, i2);
        });

        List<String> missingNumbers = new ArrayList<>();

        for (BatchFile susFile: susFiles) {
            int num = getFileNumber(susFile.getFilename());
            sequenceNumber ++;
            LOG.trace("Received " + susFile.getFilename() + " with num " + num + " expecting " + sequenceNumber);

            if (num != sequenceNumber) {
                for (int i=sequenceNumber; i<num; i++) {
                    missingNumbers.add("" + i);
                    LOG.trace("Missing " + i);
                }
                sequenceNumber = num;
            }
        }

        if (!missingNumbers.isEmpty()) {
            String msg = dbConfiguration.getConfigurationId() + " has missing " + fileType + " sequence numbers: " + String.join(", ", missingNumbers) + " in batch " + incompleteBatch.getBatchIdentifier();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);
        }
    }

    private int findPreviousSequenceNumber(String fileType, Batch lastCompleteBatch, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        int ret = 0;

        if (lastCompleteBatch == null) {
            return ret;
        }

        //first check the previous batch for that file type
        for (BatchFile batchFile: lastCompleteBatch.getBatchFiles()) {
            if (batchFile.getFileTypeIdentifier().equalsIgnoreCase(fileType)) {
                int num = getFileNumber(batchFile.getFilename());
                ret = Math.max(ret, num);
            }
        }

        //if the previous batch didn't have any of that file type, then we'll need to go back further
        if (ret == 0) {
            List<Batch> allBatches = db.getAllBatches(dbConfiguration.getConfigurationId());
            for (Batch b: allBatches) {

                //skip any newer batches
                if (b.getCompleteDate() == null
                        || b.getSequenceNumber() == null
                        || b.getSequenceNumber().intValue() >= lastCompleteBatch.getSequenceNumber().intValue()) {
                    continue;
                }

                for (BatchFile batchFile: b.getBatchFiles()) {
                    if (batchFile.getFileTypeIdentifier().equalsIgnoreCase(fileType)) {
                        int num = getFileNumber(batchFile.getFilename());
                        ret = Math.max(ret, num);
                    }
                }
            }
        }

        return ret;
    }

    /**
     * each SUS file should have a tails file with it - this fn checks to see if any are missing
     */
    private void checkAllRequiredTailsFilesArePresentInBatch(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch incompleteBatch) throws Exception {

        List<String> missingTails = new ArrayList<>();

        List<BatchFile> files = incompleteBatch.getBatchFiles();

        for (BatchFile file: files) {
            String fileType = file.getFileTypeIdentifier();

            String tailsFileType = null;

            if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSOPA)) {
                tailsFileType = BartsFilenameParser.TYPE_2_1_TAILOPA;

            } else if (fileType.equals(BartsFilenameParser.TYPE_EMERGENCY_CARE)) {
                tailsFileType = BartsFilenameParser.TYPE_EMERGENCY_CARE_TAILS;

            } else if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSAEA)) {
                tailsFileType = BartsFilenameParser.TYPE_2_1_TAILAEA;

            } else if (fileType.equals(BartsFilenameParser.TYPE_2_1_SUSIP)) {
                tailsFileType = BartsFilenameParser.TYPE_2_1_TAILIP;

            } else {
                //skip any file types that don't have a tail file
                continue;
            }

            String fileName = file.getFilename();
            int fileNum = getFileNumber(fileName);
            boolean foundTailFile = false;

            for (BatchFile otherFile: files) {
                String otherFileType = otherFile.getFileTypeIdentifier();
                if (otherFileType.equals(tailsFileType)) {

                    String otherFileName = otherFile.getFilename();
                    int otherFileNum = getFileNumber(otherFileName);
                    if (otherFileNum == fileNum) {
                        foundTailFile = true;
                    }

                }
            }

            if (!foundTailFile) {
                //Barts have started sending ECDS Tails files the day before (sometimes days before) the actual proper
                //file, so we should attempt to find the file in a previous batch and move it over to this batch
                LOG.debug("Failed to find tails file for " + fileName + " in batch " + incompleteBatch.getBatchId());
                if (!tryFindFileInPreviousBatch(db, instanceConfiguration, dbConfiguration, incompleteBatch, fileNum, tailsFileType)) {

                    //a missing tails file isn't the end of the world so don't throw an exception, but log to Slack so we can see if it's regularly happening
                    missingTails.add(fileName);
                    //throw new SftpValidationException("Missing tail file for " + fileName + " in batch " + incompleteBatch.getBatchIdentifier());
                }
            }
        }

        if (!missingTails.isEmpty()) {
            String msg = dbConfiguration.getConfigurationId() + " has missing tail file(s) for SUS file(s): " + String.join(", ", missingTails) + " in batch " + incompleteBatch.getBatchIdentifier();
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);
        }
    }

    private boolean tryFindFileInPreviousBatch(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch incompleteBatch, int fileNum, String tailsFileType) throws Exception {
        String configId = dbConfiguration.getConfigurationId();
        List<Batch> batches = db.getAllBatches(configId); //this really should be optimised to only retrieve the last 30 or so...

        //ensure batches are sorted properly
        batches.sort((o1, o2) -> {
            Integer i1 = o1.getSequenceNumber();
            Integer i2 = o2.getSequenceNumber();
            return i1.compareTo(i2);
        });

        int checked = 0;

        for (int i=batches.size()-1; i>=0; i--) {
            Batch batch = batches.get(i);
            List<BatchFile> files = batch.getBatchFiles();
            for (BatchFile file: files) {
                String type = file.getFileTypeIdentifier();
                if (!type.equals(tailsFileType)) {
                    continue;
                }

                String thisFileName = file.getFilename();
                int thisFileNum = getFileNumber(thisFileName);
                if (thisFileNum != fileNum) {
                    continue;
                }

                //if we make it here, we've found our tails file
                LOG.debug("Found tails file " + thisFileName + " in batch " + batch.getBatchId());

                //we need to copy the file from the original directory (of the date it was received)
                //to the date of the new batch
                String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
                String tempDir = instanceConfiguration.getTempDirectory();
                String configurationDir = dbConfiguration.getLocalRootPath();
                String existingBatchDir = batch.getLocalRelativePath();
                String newBatchDir = incompleteBatch.getLocalRelativePath();

                String existingPermPath = FileHelper.concatFilePath(sharedStorageDir, configurationDir, existingBatchDir, thisFileName);

                String newTempDir = FileHelper.concatFilePath(tempDir, configurationDir, newBatchDir);
                if (!new File(newTempDir).exists()) {
                    new File(newTempDir).mkdirs();
                }
                String newTempPath = FileHelper.concatFilePath(newTempDir, thisFileName);

                String newPermPath = FileHelper.concatFilePath(sharedStorageDir, configurationDir, newBatchDir, thisFileName);

                //copy from S3 to local temp dir
                LOG.debug("Copying from " + existingPermPath + " to " + newTempPath);
                InputStream is = FileHelper.readFileFromSharedStorage(existingPermPath);
                Files.copy(is, new File(newTempPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                is.close();

                //write back from local temp dir to S3
                LOG.debug("Copying from " + newTempPath + " to " + newPermPath);
                FileHelper.writeFileToSharedStorage(newPermPath, new File(newTempPath));

                //now add the tail file to the new batch
                //we need to set the last modified on the RemoteFile to match the new batch identifier to force
                //it to add the file to the new batch
                String newBatchIdentifier = incompleteBatch.getBatchIdentifier();
                LocalDate newBatchDate = BartsFilenameParser.parseBatchIdentifier(newBatchIdentifier);
                LocalDateTime newBatchDateTime = newBatchDate.atTime(0, 0);
                RemoteFile r = new RemoteFile(newPermPath, file.getRemoteSizeBytes(), newBatchDateTime);
                LOG.debug("Adding file with path " + newPermPath + " and new datetime " + newBatchDateTime);
                SftpFilenameParser filenameParser = ImplementationActivator.createFilenameParser(true, r, dbConfiguration);
                LOG.debug("New batch identifier [" + filenameParser.generateBatchIdentifier() + "] which should match incomplete batch [" + incompleteBatch.getBatchIdentifier() + "]");
                SftpFile f = new SftpFile(r, filenameParser, configurationDir);
                db.addFile(configId, f);
                LOG.debug("Added new file");

                String msg = dbConfiguration.getConfigurationId() + " " + tailsFileType + " num " + fileNum + " was found in a previous batch (" + existingBatchDir + ") and has been copied to the new batch (" + newBatchDir + ")";
                SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

                return true;
            }

            //only check the last 30 batches before failing
            checked ++;
            if (checked >= 30) {
                break;
            }
        }

        return false;
    }

    private static int getFileNumber(String fileName) {
        String digitsOnly = fileName.replaceAll("[^0-9]", "");
        return Integer.parseInt(digitsOnly);
    }


}
