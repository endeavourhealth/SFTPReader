package org.endeavourhealth.sftpreader.model;

import com.google.common.base.Strings;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.sftpreader.SftpFile;
import org.endeavourhealth.sftpreader.model.db.*;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.Date;

public class MySqlDataLayer implements DataLayerI {

    private String dbUrl;
    private String dbUsername;
    private String dbPassword;
    private String dbDriverClassName;
    private DataSource dataSource;

    public MySqlDataLayer(String dbUrl, String dbUsername, String dbPassword, String dbDriverClassName) throws Exception {
        this.dbUrl = dbUrl;
        this.dbUsername = dbUsername;
        this.dbPassword = dbPassword;
        this.dbDriverClassName = dbDriverClassName;

        Class.forName(dbDriverClassName);

        HikariDataSource hikariDataSource = new HikariDataSource();
        hikariDataSource.setJdbcUrl(dbUrl);
        hikariDataSource.setUsername(dbUsername);
        hikariDataSource.setPassword(dbPassword);
        hikariDataSource.setDriverClassName(dbDriverClassName);
        hikariDataSource.setMaximumPoolSize(5);
        hikariDataSource.setMinimumIdle(1);
        hikariDataSource.setIdleTimeout(60000);
        hikariDataSource.setPoolName("SFTPReaderDBConnectionPool");

        this.dataSource = hikariDataSource;
    }

    @Override
    public DbInstance getInstanceConfiguration(String instanceName, String hostname) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement psSelectInstance = null;
        PreparedStatement psUpdateInstance = null;
        PreparedStatement psSelectInstanceConfiguration = null;
        PreparedStatement psSelectEdsConfiguration = null;
        try {
            String sql = "SELECT hostname, http_management_port FROM instance WHERE instance_name = ?;";

            psSelectInstance = connection.prepareStatement(sql);
            psSelectInstance.setString(1, instanceName);

            ResultSet rs = psSelectInstance.executeQuery();
            if (!rs.next()) {
                throw new Exception("Instance record not found for instance name " + instanceName);
            }
            String dbHostName = rs.getString(1);
            Integer httpPort = null;
            int portNum = rs.getInt(2);
            if (!rs.wasNull()) {
                httpPort = new Integer(portNum);
            }

            //if the host name isn't set on the DB, update the record
            if (dbHostName == null) {

                sql = "UPDATE instance SET hostname = ? WHERE instance_name = ?;";

                psUpdateInstance = connection.prepareStatement(sql);
                psUpdateInstance.setString(1, hostname);
                psUpdateInstance.setString(2, instanceName);
                psUpdateInstance.executeUpdate();

            } else {
                if (!hostname.equalsIgnoreCase(dbHostName)) {
                    throw new Exception("Hostname on instance table (" + dbHostName + ") doesn't match this server's name (" + hostname + ") - is this being run on the wrong server?");
                }
            }

            DbInstance ret = new DbInstance();
            ret.setInstanceName(instanceName);
            ret.setHttpManagementPort(httpPort);

            //select the configuration IDs that this instance checks
            sql = "SELECT configuration_id FROM instance_configuration\tWHERE instance_name = ?;";

            psSelectInstanceConfiguration = connection.prepareStatement(sql);
            psSelectInstanceConfiguration.setString(1, instanceName);

            List<String> configurationIds = new ArrayList<>();
            ret.setConfigurationIds(configurationIds);

            rs = psSelectInstanceConfiguration.executeQuery();
            while (rs.next()) {
                String configurationId = rs.getString(1);
                configurationIds.add(configurationId);
            }

            if (configurationIds.isEmpty()) {
                throw new Exception("No configuration IDs associated with instance name " + instanceName + " in instance_configuration table");
            }

            //select the eds record
            sql = "SELECT eds_url, use_keycloak, keycloak_token_uri, keycloak_realm, keycloak_username, keycloak_password, keycloak_clientid, temp_directory, shared_storage_path"
                + " FROM configuration_eds;";

            psSelectEdsConfiguration = connection.prepareStatement(sql);

            rs = psSelectEdsConfiguration.executeQuery();
            if (!rs.next()) {
                throw new Exception("No configuration_eds record found");
            }

            int col = 1;

            DbInstanceEds eds = new DbInstanceEds();
            eds.setEdsUrl(rs.getString(col++));
            eds.setUseKeycloak(rs.getBoolean(col++));
            eds.setKeycloakTokenUri(rs.getString(col++));
            eds.setKeycloakRealm(rs.getString(col++));
            eds.setKeycloakUsername(rs.getString(col++));
            eds.setKeycloakPassword(rs.getString(col++));
            eds.setKeycloakClientId(rs.getString(col++));
            eds.setTempDirectory(rs.getString(col++));
            eds.setSharedStoragePath(rs.getString(col++));

            ret.setEdsConfiguration(eds);

            return ret;

        } finally {
            if (psSelectInstance != null) {
                psSelectInstance.close();
            }
            if (psUpdateInstance != null) {
                psUpdateInstance.close();
            }
            if (psSelectInstanceConfiguration != null) {
                psSelectInstanceConfiguration.close();
            }
            if (psSelectEdsConfiguration != null) {
                psSelectEdsConfiguration.close();
            }

            connection.close();
        }
    }

    @Override
    public DbConfiguration getConfiguration(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement psConfiguration = null;
        PreparedStatement psConfigurationSftp = null;
        PreparedStatement psConfigurationPgp = null;
        PreparedStatement psConfigurationKvp = null;
        PreparedStatement psConfigurationFileType = null;

        try {
            String sql = "SELECT c.configuration_id, c.configuration_friendly_name, it.interface_type_name, c.poll_frequency_seconds, c.local_root_path, c.software_content_type, c.software_version"
                    + " FROM configuration c"
                    + " INNER JOIN interface_type it"
                    + " ON c.interface_type_id = it.interface_type_id"
                    + " WHERE c.configuration_id = ?;";

            psConfiguration = connection.prepareStatement(sql);
            psConfiguration.setString(1, configurationId);

            DbConfiguration ret = null;

            ResultSet rs = psConfiguration.executeQuery();
            if (rs.next()) {
                int col = 1;

                ret = new DbConfiguration();
                ret.setConfigurationId(rs.getString(col++));
                ret.setConfigurationFriendlyName(rs.getString(col++));
                ret.setInterfaceTypeName(rs.getString(col++));
                ret.setPollFrequencySeconds(rs.getInt(col++));
                ret.setLocalRootPath(rs.getString(col++));
                ret.setSoftwareContentType(rs.getString(col++));
                ret.setSoftwareVersion(rs.getString(col++));

            } else {
                throw new PgStoredProcException("No configuration found with configuration id " + configurationId);
            }

            sql = "SELECT hostname, port, remote_path, username, client_private_key, client_private_key_password, host_public_key"
                    + " FROM configuration_sftp"
                    + " WHERE configuration_id = ?;";

            psConfigurationSftp = connection.prepareStatement(sql);
            psConfigurationSftp.setString(1, configurationId);

            rs = psConfigurationSftp.executeQuery();
            if (rs.next()) {
                int col = 1;

                DbConfigurationSftp sftpConfig = new DbConfigurationSftp();
                sftpConfig.setHostname(rs.getString(col++));
                sftpConfig.setPort(rs.getInt(col++));
                sftpConfig.setRemotePath(rs.getString(col++));
                sftpConfig.setUsername(rs.getString(col++));
                sftpConfig.setClientPrivateKey(rs.getString(col++));
                sftpConfig.setClientPrivateKeyPassword(rs.getString(col++));
                sftpConfig.setHostPublicKey(rs.getString(col++));

                ret.setSftpConfiguration(sftpConfig);

            } else {
                throw new PgStoredProcException("No SFTP configuration details found for configuration id " + configurationId);
            }

            sql = "SELECT file_extension_filter, sender_public_key, recipient_public_key, recipient_private_key, recipient_private_key_password"
                    + " FROM configuration_pgp"
                    + " WHERE configuration_id = ?;";

            psConfigurationPgp = connection.prepareStatement(sql);
            psConfigurationPgp.setString(1, configurationId);

            rs = psConfigurationPgp.executeQuery();
            if (rs.next()) {
                int col = 1;

                DbConfigurationPgp pgpConfig = new DbConfigurationPgp();
                pgpConfig.setPgpFileExtensionFilter(rs.getString(col++));
                pgpConfig.setPgpSenderPublicKey(rs.getString(col++));
                pgpConfig.setPgpRecipientPublicKey(rs.getString(col++));
                pgpConfig.setPgpRecipientPrivateKey(rs.getString(col++));
                pgpConfig.setPgpRecipientPrivateKeyPassword(rs.getString(col++));

                ret.setPgpConfiguration(pgpConfig);

            } else {
                //only Emis uses PGP so far, so no worries if this returns no rows
            }

            sql = "SELECT `key`, value FROM configuration_kvp WHERE configuration_id = ?;";
            psConfigurationKvp = connection.prepareStatement(sql);
            psConfigurationKvp.setString(1, configurationId);

            List<DbConfigurationKvp> keys = new ArrayList<>();
            ret.setDbConfigurationKvp(keys);

            rs = psConfigurationKvp.executeQuery();
            while (rs.next()) {
                int col = 1;

                DbConfigurationKvp kvp = new DbConfigurationKvp();
                kvp.setKey(rs.getString(col++));
                kvp.setValue(rs.getString(col++));

                keys.add(kvp);
            }

            sql = "SELECT ift.file_type_identifier"
                    + " FROM configuration c"
                    + " INNER JOIN interface_type it ON c.interface_type_id = it.interface_type_id"
                    + " INNER JOIN interface_file_type ift ON ift.interface_type_id = it.interface_type_id"
                    + " WHERE c.configuration_id = ?;";

            psConfigurationFileType = connection.prepareStatement(sql);
            psConfigurationFileType.setString(1, configurationId);

            List<String> fileTypes = new ArrayList<>();
            ret.setInterfaceFileTypes(fileTypes);

            rs = psConfigurationFileType.executeQuery();
            while (rs.next()) {
                String fileType = rs.getString(1);
                fileTypes.add(fileType);
            }

            return ret;

        } finally {
            if (psConfiguration != null) {
                psConfiguration.close();
            }
            if (psConfigurationSftp != null) {
                psConfigurationSftp.close();
            }
            if (psConfigurationPgp != null) {
                psConfigurationPgp.close();
            }
            if (psConfigurationKvp != null) {
                psConfigurationKvp.close();
            }
            if (psConfigurationFileType != null) {
                psConfigurationFileType.close();
            }
            connection.close();
        }
    }

    @Override
    public AddFileResult addFile(String configurationId, SftpFile sftpFile) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement psSelectInterfaceType = null;
        PreparedStatement psSelectBatch = null;
        PreparedStatement psInsertBatch = null;
        PreparedStatement psSelectBatchFile = null;
        PreparedStatement psDeleteBatchFile = null;
        PreparedStatement psInsertBatchFile = null;

        try {
            //first we need to now the interface file type for our config
            String sql = "SELECT interface_type_id FROM configuration WHERE configuration_id = ?";

            psSelectInterfaceType = connection.prepareStatement(sql);
            psSelectInterfaceType.setString(1, configurationId);

            ResultSet rs = psSelectInterfaceType.executeQuery();
            if (!rs.next()) {
                throw new Exception("Failed to find interface_type_id from configuation table for ID " + configurationId);
            }
            int interfaceTypeId = rs.getInt(1);

            //next find the batch that this file would go (or has gone) against
            sql = "SELECT batch_id FROM batch WHERE configuration_id = ? AND batch_identifier = ?;";

            psSelectBatch = connection.prepareStatement(sql);
            psSelectBatch.setString(1, configurationId);
            psSelectBatch.setString(2, sftpFile.getBatchIdentifier());

            int batchId;

            rs = psSelectBatch.executeQuery();
            if (rs.next()) {
                batchId = rs.getInt(1);

            } else {
                //if no batch ID exists, we need to create one
                sql = "INSERT INTO batch (configuration_id, batch_identifier, interface_type_id, local_relative_path, insert_date, is_complete) VALUES (?, ?, ?, ?, ?, ?);";

                psInsertBatch = connection.prepareStatement(sql);
                psInsertBatch.setString(1, configurationId);
                psInsertBatch.setString(2, sftpFile.getBatchIdentifier());
                psInsertBatch.setInt(3, interfaceTypeId);
                psInsertBatch.setString(4, sftpFile.getLocalRelativePath());
                psInsertBatch.setTimestamp(5, new java.sql.Timestamp(new Date().getTime()));
                psInsertBatch.setBoolean(6, false);
                psInsertBatch.executeUpdate();

                //then we need to select the auto-assigned batch ID
                psSelectBatch.setString(1, configurationId);
                psSelectBatch.setString(2, sftpFile.getBatchIdentifier());
                rs = psSelectBatch.executeQuery();
                if (rs.next()) {
                    batchId = rs.getInt(1);
                } else {
                    throw new Exception("Failed to find batch ID after inserting it");
                }
            }

            //now select from the batch_file to see if we've already handled this file
            sql = "SELECT batch_file_id, is_downloaded FROM batch_file WHERE batch_id = ? AND file_type_identifier = ? AND filename = ?;";

            psSelectBatchFile = connection.prepareStatement(sql);
            psSelectBatchFile.setInt(1, batchId);
            psSelectBatchFile.setString(2, sftpFile.getFileTypeIdentifier());
            psSelectBatchFile.setString(3, sftpFile.getFilename());

            int batchFileId = -1;
            boolean alreadyDownloaded = false;

            rs = psSelectBatchFile.executeQuery();
            if (rs.next()) {
                int col = 1;
                batchFileId = rs.getInt(col++);
                alreadyDownloaded = rs.getBoolean(col++);
            }

            //if we're previously added the file but not finished downloading it, delete the batch_file so we re-create it
            if (batchFileId > -1 && !alreadyDownloaded) {

                sql = "DELETE FROM batch_file WHERE batch_file_id = ?;";

                psDeleteBatchFile = connection.prepareStatement(sql);
                psDeleteBatchFile.setInt(1, batchFileId);

                psDeleteBatchFile.executeUpdate();
                batchFileId = -1;
            }

            //create the batch file if necessary
            if (batchFileId == -1) {

                sql = "INSERT INTO batch_file (batch_id, interface_type_id, file_type_identifier, insert_date, filename, remote_size_bytes, remote_created_date)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?);";

                Date remoteCreatedDate = java.sql.Timestamp.valueOf(sftpFile.getRemoteLastModifiedDate());

                psInsertBatchFile = connection.prepareStatement(sql);
                psInsertBatchFile.setInt(1, batchId);
                psInsertBatchFile.setInt(2, interfaceTypeId);
                psInsertBatchFile.setString(3, sftpFile.getFileTypeIdentifier());
                psInsertBatchFile.setTimestamp(4, new java.sql.Timestamp(new Date().getTime()));
                psInsertBatchFile.setString(5, sftpFile.getFilename());
                psInsertBatchFile.setLong(6, sftpFile.getRemoteFileSizeInBytes());
                psInsertBatchFile.setTimestamp(7, new java.sql.Timestamp(remoteCreatedDate.getTime()));
                psInsertBatchFile.executeUpdate();

                //and select the auto-assigned ID
                psSelectBatchFile.setInt(1, batchId);
                psSelectBatchFile.setString(2, sftpFile.getFileTypeIdentifier());
                psSelectBatchFile.setString(3, sftpFile.getFilename());

                rs = psSelectBatchFile.executeQuery();
                if (rs.next()) {
                    int col = 1;
                    batchFileId = rs.getInt(col++);
                    alreadyDownloaded = rs.getBoolean(col++);
                } else {
                    throw new Exception("Failed to find batch_file just inserted");
                }
            }

            AddFileResult ret = new AddFileResult();
            ret.setBatchFileId(batchFileId);
            ret.setFileAlreadyProcessed(alreadyDownloaded);
            return ret;

        } finally {
            if (psSelectInterfaceType != null) {
                psSelectInterfaceType.close();
            }
            if (psSelectBatch != null) {
                psSelectBatch.close();
            }
            if (psInsertBatch != null) {
                psInsertBatch.close();
            }
            if (psSelectBatchFile != null) {
                psSelectBatchFile.close();
            }
            if (psDeleteBatchFile != null) {
                psDeleteBatchFile.close();
            }
            if (psInsertBatchFile != null) {
                psInsertBatchFile.close();
            }

            connection.close();
        }
    }

    @Override
    public void setFileAsDownloaded(SftpFile batchFile) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch_file SET is_downloaded = ?, download_date = ? WHERE batch_file_id = ?;";

            ps = connection.prepareStatement(sql);
            ps.setBoolean(1, true);
            ps.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
            ps.setInt(3, batchFile.getBatchFileId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public boolean addUnknownFile(String configurationId, SftpFile batchFile) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement psSelect = null;
        PreparedStatement psInsert = null;
        try {
            String sql = "SELECT * FROM unknown_file WHERE configuration_id = ? AND filename = ?;";

            psSelect = connection.prepareStatement(sql);
            psSelect.setString(1, configurationId);
            psSelect.setString(2, batchFile.getFilename());

            ResultSet rs = psSelect.executeQuery();
            if (!rs.next()) {

                sql = "INSERT INTO unknown_file (configuration_id, insert_date, filename, remote_created_date, remote_size_bytes) VALUES (?, ?, ?, ?, ?);";

                Date remoteDate = java.sql.Timestamp.valueOf(batchFile.getRemoteLastModifiedDate());

                psInsert = connection.prepareStatement(sql);
                psInsert.setString(1, configurationId);
                psInsert.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
                psInsert.setString(3, batchFile.getFilename());
                psInsert.setTimestamp(4, new java.sql.Timestamp(remoteDate.getTime()));
                psInsert.setLong(5, batchFile.getRemoteFileSizeInBytes());
                psInsert.executeUpdate();

                return true;

            } else {
                return false;
            }

        } finally {
            if (psSelect != null) {
                psSelect.close();
            }
            if (psInsert != null) {
                psInsert.close();
            }
            connection.close();
        }
    }

    @Override
    public List<Batch> getIncompleteBatches(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT batch_id FROM batch WHERE configuration_id = ? AND is_complete = false;";

            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);

            List<Integer> batchIds = new ArrayList<>();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int batchId = rs.getInt(1);
                batchIds.add(new Integer(batchId));
            }

            return selectBatches(batchIds);

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    private List<Batch> selectBatches(List<Integer> batchIds) throws Exception {

        if (batchIds.isEmpty()) {
            return new ArrayList<>();
        }

        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT b.batch_id, b.batch_identifier, b.local_relative_path, b.sequence_number, "
                    + " bf.batch_file_id, bf.file_type_identifier, bf.filename, bf.remote_size_bytes, bf.is_downloaded"
                    + " FROM batch b"
                    + " LEFT OUTER JOIN batch_file bf"
                    + " ON b.batch_id = bf.batch_id"
                    + " WHERE b.batch_id IN ("+ String.join(", ", Collections.nCopies(batchIds.size(), "?")) + ");";

            ps = connection.prepareStatement(sql);

            int col = 1;
            for (Integer batchId: batchIds) {
                ps.setInt(col++, batchId.intValue());
            }

            List<Batch> ret = new ArrayList<>();
            Map<Integer, Batch> hmBatches = new HashMap<>();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                col = 1;

                //batch cols
                int batchId = rs.getInt(col++);
                String batchIdentifier = rs.getString(col++);
                String batchLocalRelativePath = rs.getString(col++);
                int sequenceNumber = rs.getInt(col++);

                //because we're doing a left outer join, we'll get multiple rows with the batch details, so use a map to handle the duplicates
                Batch batch = hmBatches.get(new Integer(batchId));
                if (batch == null) {
                    batch = new Batch();
                    batch.setBatchId(batchId);
                    batch.setBatchIdentifier(batchIdentifier);
                    batch.setLocalRelativePath(batchLocalRelativePath);
                    batch.setSequenceNumber(sequenceNumber);

                    ret.add(batch);
                    hmBatches.put(new Integer(batchId), batch);
                }

                //batch file cols
                int batchFileId = rs.getInt(col++);
                if (!rs.wasNull()) {
                    String fileTypeIdentifier = rs.getString(col++);
                    String fileName = rs.getString(col++);
                    long remoteSizeBytes = rs.getLong(col++);
                    boolean isDownloaded = rs.getBoolean(col++);

                    BatchFile batchFile = new BatchFile();
                    batchFile.setBatchId(batchId);
                    batchFile.setBatchFileId(batchFileId);
                    batchFile.setFileTypeIdentifier(fileTypeIdentifier);
                    batchFile.setFilename(fileName);
                    batchFile.setRemoteSizeBytes(remoteSizeBytes);
                    batchFile.setDownloaded(isDownloaded);

                    batch.addBatchFile(batchFile);
                }
            }

            return ret;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public Batch getLastCompleteBatch(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT b.batch_id FROM batch b"
                    + " WHERE b.configuration_id = ?"
                    + " AND sequence_number = ("
                    + " SELECT MAX(sequence_number)"
                    + " FROM batch"
                    + " WHERE configuration_id = ?"
                    + " AND is_complete = true);";

            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);
            ps.setString(2, configurationId);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int batchId = rs.getInt(1);
                List<Integer> batchIds = new ArrayList<>();
                batchIds.add(new Integer(batchId));
                List<Batch> batches = selectBatches(batchIds);
                return batches.get(0);

            } else {
                return null;
            }

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public List<Batch> getAllBatches(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT b.batch_id FROM batch b"
                    + " WHERE b.configuration_id = ?";

            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int batchId = rs.getInt(1);
                List<Integer> batchIds = new ArrayList<>();
                batchIds.add(new Integer(batchId));
                return selectBatches(batchIds);

            } else {
                return null;
            }

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public List<BatchSplit> getUnnotifiedBatchSplits(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT bs.batch_split_id, bs.batch_id, bs.local_relative_path, bs.organisation_id"
                    + " FROM batch_split bs"
                    + " INNER JOIN batch b"
                    + " ON b.batch_id = bs.batch_id"
                    + " WHERE b.configuration_id = ?"
                    + " AND b.is_complete = true"
                    + " AND bs.have_notified = false;";

            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();

            List<BatchSplit> ret = new ArrayList<>();
            Set<Integer> batchIds = new HashSet<>();

            while (rs.next()) {
                int col = 1;

                BatchSplit batchSplit = new BatchSplit();
                batchSplit.setBatchSplitId(rs.getInt(col++));
                batchSplit.setBatchId(rs.getInt(col++));
                batchSplit.setLocalRelativePath(rs.getString(col++));
                batchSplit.setOrganisationId(rs.getString(col++));

                ret.add(batchSplit);
                batchIds.add(new Integer(batchSplit.getBatchId()));
            }

            //then retrieve the batches (and batch files) for each batch we've identified, and set the batches on the batch splits
            Map<Integer, Batch> batchMap = new HashMap<>();
            List<Integer> batchIdsList = new ArrayList<>(batchIds);
            List<Batch> batches = selectBatches(batchIdsList);
            for (Batch batch: batches) {
                int batchId = batch.getBatchId();
                batchMap.put(new Integer(batchId), batch);
            }

            for (BatchSplit batchSplit: ret) {
                int batchId = batchSplit.getBatchId();
                Batch batch = batchMap.get(new Integer(batchId));
                batchSplit.setBatch(batch);
            }

            return ret;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public List<UnknownFile> getUnknownFiles(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT unknown_file_id, insert_date, filename, remote_created_date, remote_size_bytes FROM unknown_file WHERE configuration_id = ?;";
            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();

            List<UnknownFile> ret = new ArrayList<>();

            while (rs.next()) {
                int col = 1;
                int unknownFileId = rs.getInt(col++);
                Date insertDate = rs.getDate(col++);
                String fileName = rs.getString(col++);
                Date remoteCreatedDate = rs.getDate(col++);
                long remoteSizeBytes = rs.getLong(col++);

                LocalDateTime insertDateLocalDateTime = new java.sql.Timestamp(
                        insertDate.getTime()).toLocalDateTime();

                LocalDateTime remoteCreatedDateLocalDateTime = new java.sql.Timestamp(
                        remoteCreatedDate.getTime()).toLocalDateTime();

                UnknownFile unknownFile = new UnknownFile();
                unknownFile.setUnknownFileId(unknownFileId);
                unknownFile.setInsertDate(insertDateLocalDateTime);
                unknownFile.setFilename(fileName);
                unknownFile.setRemoteCreatedDate(remoteCreatedDateLocalDateTime);
                unknownFile.setRemoteSizeBytes(remoteSizeBytes);

                ret.add(unknownFile);
            }

            return ret;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void setBatchAsComplete(Batch batch) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch SET is_complete = ?, complete_date = ? WHERE batch_id = ?;";

            ps = connection.prepareStatement(sql);
            ps.setBoolean(1, true);
            ps.setTimestamp(2, new java.sql.Timestamp(new Date().getTime()));
            ps.setInt(3, batch.getBatchId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void setBatchSequenceNumber(Batch batch, int sequenceNumber) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch SET sequence_number = ? WHERE batch_id = ?;";

            ps = connection.prepareStatement(sql);
            ps.setInt(1, sequenceNumber);
            ps.setInt(2, batch.getBatchId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void addBatchNotification(int batchId, int batchSplitId, String configurationId, UUID messageId, String outboundMessage, String inboundMessage, boolean wasSuccess, String errorText) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement psInsert = null;
        PreparedStatement psUpdate = null;
        try {
            String sql = "INSERT INTO notification_message (batch_id, batch_split_id, configuration_id, message_uuid, timestamp, outbound, inbound, was_success, error_text) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);";

            Date now = new Date();

            psInsert = connection.prepareStatement(sql);
            psInsert.setInt(1, batchId);
            psInsert.setInt(2, batchSplitId);
            psInsert.setString(3, configurationId);
            psInsert.setString(4, messageId.toString());
            psInsert.setTimestamp(5, new java.sql.Timestamp(now.getTime()));
            psInsert.setString(6, outboundMessage);
            psInsert.setString(7, inboundMessage);
            psInsert.setBoolean(8, wasSuccess);
            psInsert.setString(9, errorText);

            psInsert.executeUpdate();

            //if successful, update the batch split too
            if (wasSuccess) {
                sql = "UPDATE batch_split SET have_notified = ?, notification_date = ? WHERE batch_split_id = ?";

                psUpdate = connection.prepareStatement(sql);
                psUpdate.setBoolean(1, true);
                psUpdate.setTimestamp(2, new java.sql.Timestamp(now.getTime()));
                psUpdate.setInt(3, batchSplitId);

                psUpdate.executeUpdate();
            }

        } finally {
            if (psInsert != null) {
                psInsert.close();
            }
            if (psUpdate != null) {
                psUpdate.close();
            }
            connection.close();
        }
    }

    @Override
    public void addBatchSplit(BatchSplit batchSplit) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO batch_split (batch_id, configuration_id, local_relative_path, organisation_id) VALUES (?, ?, ?, ?);";

            ps = connection.prepareStatement(sql);
            ps.setInt(1, batchSplit.getBatchId());
            ps.setString(2, batchSplit.getConfigurationId());
            ps.setString(3, batchSplit.getLocalRelativePath());
            ps.setString(4, batchSplit.getOrganisationId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void deleteBatchSplits(Batch batch) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "DELETE FROM batch_split WHERE batch_id = ?;";

            ps = connection.prepareStatement(sql);
            ps.setInt(1, batch.getBatchId());
            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }


    }

    @Override
    public List<BatchSplit> getBatchSplitsForBatch(int queryBatchId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * FROM batch_split WHERE batch_id = ?;";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, queryBatchId);

            ResultSet rs = ps.executeQuery();

            List<BatchSplit> ret = new ArrayList<>();

            while (rs.next()) {
                int col = 1;
                int batchSplitId = rs.getInt(col++);
                int batchId = rs.getInt(col++);
                String configurationId = rs.getString(col++);
                String localRelativePath = rs.getString(col++);
                String organisationId = rs.getString(col++);
                boolean haveNotified = rs.getBoolean(col++);
                Date notificationDate = rs.getDate(col++);

                BatchSplit batchSplit = new BatchSplit();
                batchSplit.setBatchSplitId(batchSplitId);
                batchSplit.setBatchId(batchId);
                batchSplit.setConfigurationId(configurationId);
                batchSplit.setLocalRelativePath(localRelativePath);
                batchSplit.setOrganisationId(organisationId);
                batchSplit.setHaveNotified(haveNotified);
                batchSplit.setNotificationDate(notificationDate);

                ret.add(batchSplit);
            }

            return ret;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void addEmisOrganisationMap(EmisOrganisationMap mapping) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO emis_organisation_map (guid, name, ods_code) VALUES (?, ?, ?) "
                    + " ON DUPLICATE KEY UPDATE"
                    + " name = VALUES(name),"
                    + " ods_code = VALUES(ods_code)";

            ps = connection.prepareStatement(sql);
            ps.setString(1, mapping.getGuid());
            ps.setString(2, mapping.getName());
            ps.setString(3, mapping.getOdsCode());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public List<EmisOrganisationMap> getEmisOrganisationMapsForOdsCode(String queryOdsCode) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * FROM emis_organisation_map WHERE ods_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, queryOdsCode);

            ResultSet rs = ps.executeQuery();

            List<EmisOrganisationMap> ret = new ArrayList<>();

            //we have multiple names for some orgs in production (e.g. F84636),
            //so return the name with the longest length (for the sake of having some way to choose
            //something more interesting that just "The Surgery")
            while (rs.next()) {
                int col = 1;
                String guid = rs.getString(col++);
                String name = rs.getString(col++);
                String odsCode = rs.getString(col++);

                EmisOrganisationMap m = new EmisOrganisationMap();
                m.setGuid(guid);
                m.setName(name);
                m.setOdsCode(odsCode);

                ret.add(m);
            }

            return ret;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public EmisOrganisationMap getEmisOrganisationMap(String guid) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * FROM emis_organisation_map WHERE guid = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, guid);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int col = 1;
                EmisOrganisationMap ret = new EmisOrganisationMap();
                ret.setGuid(rs.getString(col++));
                ret.setName(rs.getString(col++));
                ret.setOdsCode(rs.getString(col++));
                return ret;
            } else {
                return null;
            }

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void addTppOrganisationMap(TppOrganisationMap mapping) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO tpp_organisation_map (ods_code, name) VALUES (?, ?) "
                    + " ON DUPLICATE KEY UPDATE"
                    + " name = VALUES(name)";

            ps = connection.prepareStatement(sql);
            ps.setString(1, mapping.getOdsCode());
            ps.setString(2, mapping.getName());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public TppOrganisationMap getTppOrgNameFromOdsCode(String queryOdsCode) throws Exception {

        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * FROM tpp_organisation_map WHERE ods_code = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, queryOdsCode);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                int col = 1;
                TppOrganisationMap ret = new TppOrganisationMap();
                ret.setOdsCode(rs.getString(col++));
                ret.setName(rs.getString(col++));
                return ret;
            } else {
                return null;
            }

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public ConfigurationLockI createConfigurationLock(String lockName) throws Exception {

        //the lock uses a single connection over a long period of time, so we want a connection
        //that's not managed by the connection pool
        MysqlDataSource nonPooledDataSource = new MysqlDataSource();
        nonPooledDataSource.setURL(dbUrl);
        nonPooledDataSource.setUser(dbUsername);
        nonPooledDataSource.setPassword(dbPassword);

        Connection connection = nonPooledDataSource.getConnection();

        return new MySqlConfigurationLock(lockName, connection);
    }
}
