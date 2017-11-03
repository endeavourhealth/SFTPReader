package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class VisionSftpBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(VisionSftpBatchValidator.class);

    @Override
    public void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch, DbConfiguration dbConfiguration, DataLayer db) throws SftpValidationException {
        Validate.notNull(incompleteBatches, "incompleteBatches is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        for (Batch incompleteBatch : incompleteBatches) {
            // There will only be 5 zip files or less in each batch for Vision practices
            int fileCount = incompleteBatch.getBatchFiles().size();
            if (fileCount > 5) {
                throw new SftpValidationException("Incorrect number of files ("+Integer.toString(fileCount)+") in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
            }
        }
    }
}
