package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

public abstract class SftpNotificationCreator {
    public abstract String createNotificationMessage(String organisationId, DataLayer db, DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception;
}
