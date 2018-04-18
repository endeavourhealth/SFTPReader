package org.endeavourhealth.sftpreader.model;

import org.endeavourhealth.common.postgres.PgAppLock.PgAppLock;
import org.slf4j.LoggerFactory;

import java.sql.Connection;

public class PostgresConfigurationLock implements ConfigurationLockI {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PostgresConfigurationLock.class);

    private PgAppLock lock = null;

    public PostgresConfigurationLock(String lockName, Connection connection) throws Exception {

        LOG.info("Connection open 3 " + !connection.isClosed());

        try {
            this.lock = new PgAppLock(lockName, connection);
        }
        catch (Exception ex) {
            LOG.info("Connection open 4 " + !connection.isClosed());
            throw ex;
        }
    }

    @Override
    public void releaseLock() throws Exception {
        this.lock.releaseLock();
    }
}
