package org.endeavourhealth.sftpreader.implementations.bhrut;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BhrutBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(BhrutBatchValidator.class);

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        // There will only be 6 csv files or less in each batch bhrut extracts
        int fileCount = incompleteBatch.getBatchFiles().size();
        if (fileCount > 6) {
            throw new SftpValidationException("Incorrect number of files ("+Integer.toString(fileCount)+") in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

        return true;
    }

}
