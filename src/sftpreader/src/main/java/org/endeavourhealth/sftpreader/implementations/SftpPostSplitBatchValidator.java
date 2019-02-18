package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

public abstract class SftpPostSplitBatchValidator {

    public abstract void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch,
                                                   DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                                   DataLayerI db) throws Exception;
}
