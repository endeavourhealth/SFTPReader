package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.implementations.SftpHouseKeeper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public class TppHouseKeeper extends SftpHouseKeeper {
    @Override
    public void performHouseKeeping(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) {
        //no housekeeping to do
    }
}
