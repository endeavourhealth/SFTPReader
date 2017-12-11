package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class BartsSftpBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpBatchValidator.class);

    @Override
    public void validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayer db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        // All CDS/SUS files must have a matching Tails file
        checkAllRequiredTailsFilesArePresentInBatch(incompleteBatch, dbConfiguration);
    }


    /*
      CDS/SUS batches should only have two files - one primary and one tails
     */
    private void checkAllRequiredTailsFilesArePresentInBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
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

    }

}
