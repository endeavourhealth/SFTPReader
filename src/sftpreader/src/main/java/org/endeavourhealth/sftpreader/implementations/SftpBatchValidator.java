package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

public abstract class SftpBatchValidator {

    //validates that a batch is valid - return false to indicate it's not valid but
    //not log an exception (e.g. if the batch isn't valid but we expect it to be valid later),
    //or throw a SftpValidationException to log an error
    public abstract boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch,
                                         DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                          DataLayerI db) throws SftpValidationException;
}
