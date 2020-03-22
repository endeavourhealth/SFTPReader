package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public class TppNotificationCreator extends SftpNotificationCreator {

    @Override
    public PayloadWrapper createNotificationMessage(String organisationId, DataLayerI db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, db, batchSplit, "csv");
    }

}
