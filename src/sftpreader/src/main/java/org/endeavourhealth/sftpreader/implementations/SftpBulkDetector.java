package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public abstract class SftpBulkDetector {

    public abstract boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                          DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception;
}
