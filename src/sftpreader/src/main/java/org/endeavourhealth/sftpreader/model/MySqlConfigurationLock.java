package org.endeavourhealth.sftpreader.model;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySqlConfigurationLock implements ConfigurationLockI {

    private String lockName;
    private Connection connection;

    public MySqlConfigurationLock(String lockName, Connection connection) throws Exception {
        this.lockName = lockName;
        this.connection = connection;

        lock();
    }

    private void lock() throws Exception {

        PreparedStatement ps = null;
        try {
            String sql = "SELECT GET_LOCK(?, 1);";
            ps = connection.prepareStatement(sql);
            ps.setString(1, lockName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int result = rs.getInt(1);
                if (result == 0) {
                    throw new Exception("Failed to get DB lock on " + lockName + " - possibly running multiple SFTP Readers?");
                }

            } else {
                throw new Exception("No results in GET_LOCK result set");
            }

        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public void releaseLock() throws Exception {

        PreparedStatement ps = null;
        try {
            String sql = "SELECT RELEASE_LOCK(?);";
            ps = connection.prepareStatement(sql);
            ps.setString(1, lockName);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int result = rs.getInt(1);
                if (result == 0) {
                    throw new Exception("Failed to release DB lock on " + lockName + " - possibly running multiple SFTP Readers?");
                }

            } else {
                throw new Exception("No results in RELEASE_LOCK result set");
            }

        } finally {
            if (ps != null) {
                ps.close();
            }

            //and close the connection, since we're finished
            connection.close();
        }


    }
}
