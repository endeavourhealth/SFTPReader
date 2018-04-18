package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.util.*;

public class BartsSftpBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpBatchValidator.class);

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        LocalDate batchDate = BartsSftpFilenameParser.parseBatchIdentifier(incompleteBatch.getBatchIdentifier());

        //validate it's past 3pm if the batch is for today, because Barts drip files up over the morning. If before 3pm, just return false
        //so the batch silently fails validation and isn't processed any further
        LocalDate today = LocalDate.now();
        if (!batchDate.isBefore(today)) {
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTime(new Date());
            int hour = cal.get(Calendar.HOUR_OF_DAY);
            if (hour < 15) {
                return false;
            }
        }

        // All CDS/SUS files must have a matching Tails file
        checkAllRequiredTailsFilesArePresentInBatch(incompleteBatch, dbConfiguration);

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

    private void checkOutpatientFilesPresent(Batch incompleteBatch) throws SftpValidationException {

        boolean gotOpaFile = false;

        for (BatchFile file: incompleteBatch.getBatchFiles()) {
            String fileType = file.getFileTypeIdentifier();

            if (fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSOPA)) {
                gotOpaFile = true;
            }
        }

        if (!gotOpaFile) {
            throw new SftpValidationException("No SUSOPA file found in batch " + incompleteBatch.getBatchIdentifier());
        }
    }

    private void checkAllRequiredTailsFilesArePresentInBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {

        List<BatchFile> files = incompleteBatch.getBatchFiles();

        for (BatchFile file: files) {
            String fileType = file.getFileTypeIdentifier();

            //skip any file types that don't have a tail file
            if (!fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSAEA)
                    && !fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSOPA)
                    && !fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSIP)) {
                continue;
            }

            String fileName = file.getFilename();
            String extension = FilenameUtils.getExtension(fileName);

            boolean foundTailFile = false;

            for (BatchFile otherFile: files) {
                String otherFileType = otherFile.getFileTypeIdentifier();

                //make sure we're matching the tail file to the file type
                if (fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSAEA)
                        && !otherFileType.equalsIgnoreCase(BartsSftpFilenameParser.TYPE_2_1_TAILAEA)) {
                    continue;
                }

                if (fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSOPA)
                        && !otherFileType.equalsIgnoreCase(BartsSftpFilenameParser.TYPE_2_1_TAILOPA)) {
                    continue;
                }

                if (fileType.equals(BartsSftpFilenameParser.TYPE_2_1_SUSIP)
                        && !otherFileType.equalsIgnoreCase(BartsSftpFilenameParser.TYPE_2_1_TAILIP)) {
                    continue;
                }

                //the tail file has the same extension as the main file
                String otherFileName = otherFile.getFilename();
                String otherExtension = FilenameUtils.getExtension(otherFileName);
                if (!otherExtension.equals(extension)) {
                    continue;
                }

                foundTailFile = true;
            }

            if (!foundTailFile) {
                throw new SftpValidationException("Missing tail file for " + fileName + " in batch " + incompleteBatch.getBatchIdentifier());
            }
        }
    }

    /*
      CDS/SUS batches should only have two files - one primary and one tails
     */
    /*private void checkAllRequiredTailsFilesArePresentInBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
        BatchFile incompleteBatchFile = incompleteBatch.getBatchFiles().get(0);

        if ((incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSOPA) == 0) ||
                (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSAEA) == 0) ||
                        (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSIP) == 0) ) {

            // Only two files ?
            if (incompleteBatch.getBatchFiles().size() != 2) {
                throw new SftpValidationException("Incorrect number of files in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
            }

            // Other file a Tail file ?
            incompleteBatchFile = incompleteBatch.getBatchFiles().get(1);
            if ((incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILOPA) != 0) &&
                    (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILAEA) != 0) &&
                    (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILIP) != 0) ) {
                throw new SftpValidationException("Matching Tail file not found. Batch identifier = " + incompleteBatch.getBatchIdentifier());
            }

        } else {
            if ((incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILOPA) == 0) ||
                    (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILAEA) == 0) ||
                    (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_TAILIP) == 0) ) {

                // Only two files ?
                if (incompleteBatch.getBatchFiles().size() != 2) {
                    throw new SftpValidationException("Incorrect number of files in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
                }

                // Other file a primary file ?
                incompleteBatchFile = incompleteBatch.getBatchFiles().get(1);
                if ((incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSOPA) != 0) &&
                        (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSAEA) != 0) &&
                        (incompleteBatchFile.getFileTypeIdentifier().compareTo(BartsSftpFilenameParser.FILE_TYPE_SUSIP) != 0)) {
                    throw new SftpValidationException("Matching primary file not found. Batch identifier = " + incompleteBatch.getBatchIdentifier());
                }
            }
        }

    }*/

}
