package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.util.List;

public class BartsSftpBatchValidator extends SftpBatchValidator {

    @Override
    public void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
        Validate.notNull(incompleteBatches, "incompleteBatches is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");


        for (Batch incompleteBatch : incompleteBatches) {
            checkAllFilesArePresentInBatch(incompleteBatch, dbConfiguration);
        }
    }


    private void checkAllFilesArePresentInBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
        if (incompleteBatch.getBatchFiles().size() != 1)
            throw new SftpValidationException("Incorrect number of files in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
    }
}
