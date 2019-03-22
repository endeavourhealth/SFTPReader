package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public abstract class SftpHouseKeeper {

    public abstract void performHouseKeeping(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception;
}
