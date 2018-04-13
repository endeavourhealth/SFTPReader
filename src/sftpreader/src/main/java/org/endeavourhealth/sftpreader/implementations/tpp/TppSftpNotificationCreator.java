package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public class TppSftpNotificationCreator extends SftpNotificationCreator {

    @Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, "csv");
    }

}
