package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.util.List;

public abstract class SftpBatchSplitter {
    public abstract List<BatchSplit> splitBatch(Batch batch,
                                                DataLayer db,
                                                DbInstanceEds instanceConfiguration,
                                                DbConfiguration dbConfiguration) throws Exception;
}
