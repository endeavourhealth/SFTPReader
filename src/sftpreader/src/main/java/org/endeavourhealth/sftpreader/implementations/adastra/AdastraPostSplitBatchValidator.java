package org.endeavourhealth.sftpreader.implementations.adastra;

import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

public class AdastraPostSplitBatchValidator extends SftpPostSplitBatchValidator {
    @Override
    public void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

    }
}
