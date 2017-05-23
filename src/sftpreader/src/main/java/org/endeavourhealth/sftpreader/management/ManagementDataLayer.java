package org.endeavourhealth.sftpreader.management;

import org.endeavourhealth.common.postgres.PgResultSet;
import org.endeavourhealth.common.postgres.PgStoredProc;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.sftpreader.management.model.Instance;

import javax.sql.DataSource;
import java.util.List;

public class ManagementDataLayer {
    private DataSource dataSource;

    public ManagementDataLayer(javax.sql.DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<Instance> getInstances() throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("management.get_instances");

        return pgStoredProc.executeQuery((resultSet) ->
                new Instance()
                        .setInstanceName(resultSet.getString("instance_name"))
                        .setHostname(resultSet.getString("hostname"))
                        .setHttpManagementPort(PgResultSet.getInteger(resultSet, "http_management_port"))
                        .setLastConfigGetDate(PgResultSet.getLocalDateTime(resultSet, "last_config_get_date")));
    }
}
