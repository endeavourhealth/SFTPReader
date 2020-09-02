package org.endeavourhealth.sftpreader.model;

import com.mysql.cj.jdbc.MysqlDataSource;
import com.zaxxer.hikari.HikariDataSource;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.SftpFile;
import org.endeavourhealth.sftpreader.model.db.*;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.*;

public class MySqlDataLayer implements DataLayerI {

    public MySqlDataLayer() {}

    @Override
    public DbInstance getInstanceConfiguration(String instanceName, String hostname) throws Exception {
        Connection connection = getConnection();
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
            sql = "SELECT configuration_id FROM instance_configuration WHERE instance_name = ?;";

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
        Connection connection = getConnection();
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

    private int findInterfaceTypeId(String configurationId) throws Exception {
        Connection connection = getConnection();
        PreparedStatement psSelectInterfaceType = null;
        try {
            //first we need to now the interface file type for our config
            String sql = "SELECT interface_type_id FROM configuration WHERE configuration_id = ?";

            psSelectInterfaceType = connection.prepareStatement(sql);
            psSelectInterfaceType.setString(1, configurationId);

            ResultSet rs = psSelectInterfaceType.executeQuery();
            if (!rs.next()) {
                throw new Exception("Failed to find interface_type_id from configuation table for ID " + configurationId);
            }
            return rs.getInt(1);

        } finally {
            if (psSelectInterfaceType != null) {
                psSelectInterfaceType.close();
            }
            connection.close();
        }
    }

    private Map<Integer, Boolean> findBatches(String configurationId, String batchIdentifier) throws Exception {
        Connection connection = getConnection();
        PreparedStatement psSelectBatch = null;
        try {

            Map<Integer, Boolean> ret = new HashMap<>();

            //next find the batch that this file would go (or has gone) against
            String sql = "SELECT batch_id, is_complete FROM batch WHERE configuration_id = ? AND batch_identifier = ?";
            psSelectBatch = connection.prepareStatement(sql);

            int col = 1;
            psSelectBatch.setString(col++, configurationId);
            psSelectBatch.setString(col++, batchIdentifier);

            //go through the batches to find one that isn't marked as complete
            ResultSet rs = psSelectBatch.executeQuery();
            while (rs.next()) {

                col = 1;
                int id = rs.getInt(col++);
                boolean isComplete = rs.getBoolean(col++);

                ret.put(new Integer(id), new Boolean(isComplete));
            }

            return ret;

        } finally {
            if (psSelectBatch != null) {
                psSelectBatch.close();
            }
            connection.close();
        }
    }

    private AddFileResult findExistingBatchFile(int batchId, String fileType, String fileName) throws Exception {
        Connection connection = getConnection();
        PreparedStatement psSelectBatchFile = null;
        PreparedStatement psDeleteBatchFile = null;
        try {

            //now select from the batch_file to see if we've already handled this file
            String sql = "SELECT batch_file_id, is_downloaded FROM batch_file WHERE batch_id = ? AND file_type_identifier = ? AND filename = ?;";

            psSelectBatchFile = connection.prepareStatement(sql);

            int col = 1;
            psSelectBatchFile.setInt(col++, batchId);
            psSelectBatchFile.setString(col++, fileType);
            psSelectBatchFile.setString(col++, fileName);

            ResultSet rs = psSelectBatchFile.executeQuery();
            if (rs.next()) {
                col = 1;
                int batchFileId = rs.getInt(col++);
                boolean alreadyDownloaded = rs.getBoolean(col++);

                if (alreadyDownloaded) {
                    AddFileResult ret = new AddFileResult();
                    ret.setBatchFileId(batchFileId);
                    ret.setFileAlreadyDownloaded(true);
                    return ret;
                }

                //if we're previously added the file but not finished downloading it, delete the batch_file so we re-create it
                sql = "DELETE FROM batch_file WHERE batch_file_id = ?;";

                psDeleteBatchFile = connection.prepareStatement(sql);
                psDeleteBatchFile.setInt(1, batchFileId);

                psDeleteBatchFile.executeUpdate();
            }

            return null;

        } finally {
            if (psSelectBatchFile != null) {
                psSelectBatchFile.close();
            }
            if (psDeleteBatchFile != null) {
                psDeleteBatchFile.close();
            }
            connection.close();
        }
    }


    private int createBatch(String configurationId, String batchIdentifier, int interfaceTypeId, String localPath) throws Exception {



        Connection connection = getConnection();
        PreparedStatement psInsertBatch = null;
        PreparedStatement psLastId = null;
        try {
            String sql = "INSERT INTO batch (configuration_id, batch_identifier, interface_type_id, local_relative_path, insert_date, is_complete) VALUES (?, ?, ?, ?, ?, ?);";

            psInsertBatch = connection.prepareStatement(sql);
            psInsertBatch.setString(1, configurationId);
            psInsertBatch.setString(2, batchIdentifier);
            psInsertBatch.setInt(3, interfaceTypeId);
            psInsertBatch.setString(4, localPath);
            psInsertBatch.setTimestamp(5, new java.sql.Timestamp(new Date().getTime()));
            psInsertBatch.setBoolean(6, false);
            psInsertBatch.executeUpdate();

            //then we need to select the auto-assigned batch ID
            sql = "SELECT LAST_INSERT_ID()";
            psLastId = connection.prepareStatement(sql);

            ResultSet rs = psLastId.executeQuery();
            rs.next();
            return rs.getInt(1);

        } finally {
            if (psInsertBatch != null) {
                psInsertBatch.close();
            }
            if (psLastId != null) {
                psLastId.close();
            }
            connection.close();
        }
    }

    private AddFileResult createBatchFile(int batchId, int interfaceTypeId, String fileType, String fileName, long fileSizeBytes, Date fileCreatedDate) throws Exception {
        Connection connection = getConnection();
        PreparedStatement psInsertBatchFile = null;
        PreparedStatement psLastId = null;
        try {

            String sql = "INSERT INTO batch_file (batch_id, interface_type_id, file_type_identifier, insert_date, filename, remote_size_bytes, remote_created_date)"
                    + " VALUES (?, ?, ?, ?, ?, ?, ?);";

            psInsertBatchFile = connection.prepareStatement(sql);

            int col = 1;
            psInsertBatchFile.setInt(col++, batchId);
            psInsertBatchFile.setInt(col++, interfaceTypeId);
            psInsertBatchFile.setString(col++, fileType);
            psInsertBatchFile.setTimestamp(col++, new java.sql.Timestamp(new Date().getTime()));
            psInsertBatchFile.setString(col++, fileName);
            psInsertBatchFile.setLong(col++, fileSizeBytes);
            psInsertBatchFile.setTimestamp(col++, new java.sql.Timestamp(fileCreatedDate.getTime()));
            psInsertBatchFile.executeUpdate();

            //then we need to select the auto-assigned batch ID
            sql = "SELECT LAST_INSERT_ID()";
            psLastId = connection.prepareStatement(sql);

            ResultSet rs = psLastId.executeQuery();
            rs.next();
            int fileId = rs.getInt(1);

            AddFileResult ret = new AddFileResult();
            ret.setBatchFileId(fileId);
            ret.setFileAlreadyDownloaded(false);
            return ret;

        } finally {
            if (psInsertBatchFile != null) {
                psInsertBatchFile.close();
            }
            if (psLastId != null) {
                psLastId.close();
            }
            connection.close();
        }
    }

    @Override
    public AddFileResult addFile(String configurationId, SftpFile sftpFile) throws Exception {

        //find all existing batches for the identifier
        String batchIdentifier = sftpFile.getBatchIdentifier();
        Map<Integer, Boolean> batches = findBatches(configurationId, batchIdentifier);

        String fileType = sftpFile.getFileTypeIdentifier();
        String fileName = sftpFile.getFilename();

        //see if the file already exists against one of those batches
        for (Integer batchId: batches.keySet()) {
            AddFileResult r = findExistingBatchFile(batchId.intValue(), fileType, fileName);
            if (r != null) {
                return r;
            }
        }

        //find a suitable batch, that's not been completed yet
        int batchIdToUse = -1;

        for (Integer batchId: batches.keySet()) {

            //any that's already completed will have been sent to the messaging API, so can't be added to
            boolean isComplete = batches.get(batchId).booleanValue();
            if (!isComplete) {
                batchIdToUse = batchId.intValue();
            }
        }

        int interfaceTypeId = findInterfaceTypeId(configurationId);

        //if no batch ID exists, we need to create one
        if (batchIdToUse == -1) {
            String localPath = sftpFile.getLocalRelativePath();
            batchIdToUse = createBatch(configurationId, batchIdentifier, interfaceTypeId, localPath);
        }

        long fileSizeBytes = sftpFile.getRemoteFileSizeInBytes();
        Date fileCreatedDate = java.sql.Timestamp.valueOf(sftpFile.getRemoteLastModifiedDate());
        return createBatchFile(batchIdToUse, interfaceTypeId, fileType, fileName, fileSizeBytes, fileCreatedDate);
    }

    @Override
    public void setFileAsDownloaded(int batchFileId, boolean downloaded) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch_file SET is_downloaded = ?, download_date = ? WHERE batch_file_id = ?;";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setBoolean(col++, downloaded);
            if (downloaded) {
                ps.setTimestamp(col++, new java.sql.Timestamp(new Date().getTime()));
            } else {
                ps.setNull(col++, Types.DATE);
            }
            ps.setInt(col++, batchFileId);

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void setFileAsDeleted(BatchFile batchFile) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch_file SET is_deleted = ? WHERE batch_file_id = ?;";

            ps = connection.prepareStatement(sql);
            ps.setBoolean(1, true);
            ps.setInt(2, batchFile.getBatchFileId());

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
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

        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT b.batch_id, b.batch_identifier, b.local_relative_path, b.insert_date, b.sequence_number, b.complete_date,"
                    + " bf.batch_file_id, bf.file_type_identifier, bf.filename, bf.remote_size_bytes, bf.is_downloaded, bf.is_deleted"
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
                Date insertDate = new java.util.Date(rs.getTimestamp(col++).getTime());

                //need to handle possible null of sequence number
                Integer sequenceNumber = null;
                int i = rs.getInt(col++);
                if (!rs.wasNull()) {
                    sequenceNumber = new Integer(i);
                }

                Date completeDate = rs.getDate(col++);

                //because we're doing a left outer join, we'll get multiple rows with the batch details, so use a map to handle the duplicates
                Batch batch = hmBatches.get(new Integer(batchId));
                if (batch == null) {
                    batch = new Batch();
                    batch.setBatchId(batchId);
                    batch.setBatchIdentifier(batchIdentifier);
                    batch.setLocalRelativePath(batchLocalRelativePath);
                    batch.setInsertDate(insertDate);
                    batch.setSequenceNumber(sequenceNumber);
                    batch.setCompleteDate(completeDate);

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
                    boolean isDeleted = rs.getBoolean(col++);

                    BatchFile batchFile = new BatchFile();
                    batchFile.setBatchId(batchId);
                    batchFile.setBatchFileId(batchFileId);
                    batchFile.setFileTypeIdentifier(fileTypeIdentifier);
                    batchFile.setFilename(fileName);
                    batchFile.setRemoteSizeBytes(remoteSizeBytes);
                    batchFile.setDownloaded(isDownloaded);
                    batchFile.setDeleted(isDeleted);

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT b.batch_id FROM batch b"
                    + " WHERE b.configuration_id = ?";

            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);

            List<Integer> batchIds = new ArrayList<>();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                int batchId = rs.getInt(1);
                batchIds.add(new Integer(batchId));
            }

            if (!batchIds.isEmpty()) {
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
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT bs.batch_split_id, bs.batch_id, bs.local_relative_path, bs.organisation_id, bs.is_bulk"
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
                batchSplit.setConfigurationId(configurationId);

                batchSplit.setBatchSplitId(rs.getInt(col++));
                batchSplit.setBatchId(rs.getInt(col++));
                batchSplit.setLocalRelativePath(rs.getString(col++));
                batchSplit.setOrganisationId(rs.getString(col++));
                batchSplit.setBulk(rs.getBoolean(col++));

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
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
    public void setBatchSequenceNumber(Batch batch, Integer sequenceNumber) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE batch SET sequence_number = ? WHERE batch_id = ?;";

            ps = connection.prepareStatement(sql);

            int col = 1;
            if (sequenceNumber == null) {
                ps.setNull(col++, Types.INTEGER);
            } else {
                ps.setInt(col++, sequenceNumber);
            }
            ps.setInt(col++, batch.getBatchId());

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO batch_split (batch_id, configuration_id, local_relative_path, organisation_id, is_bulk) VALUES (?, ?, ?, ?, ?)";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setInt(col++, batchSplit.getBatchId());
            ps.setString(col++, batchSplit.getConfigurationId());
            ps.setString(col++, batchSplit.getLocalRelativePath());
            ps.setString(col++, batchSplit.getOrganisationId());
            ps.setBoolean(col++, batchSplit.isBulk());

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT batch_split_id, batch_id, configuration_id, local_relative_path, organisation_id, have_notified, notification_date, is_bulk"
                        + " FROM batch_split"
                        + " WHERE batch_id = ?";
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
                boolean isBulk = rs.getBoolean(col++);

                BatchSplit batchSplit = new BatchSplit();
                batchSplit.setBatchSplitId(batchSplitId);
                batchSplit.setBatchId(batchId);
                batchSplit.setConfigurationId(configurationId);
                batchSplit.setLocalRelativePath(localRelativePath);
                batchSplit.setOrganisationId(organisationId);
                batchSplit.setHaveNotified(haveNotified);
                batchSplit.setNotificationDate(notificationDate);
                batchSplit.setBulk(isBulk);

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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
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
        Connection connection = getConnection();
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
    public TppOrganisationMap getTppOrgNameFromOdsCode(String queryOdsCode) throws Exception {

        Connection connection = getConnection();
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
    public void addTppOrganisationGmsRegistrationMap(TppOrganisationGmsRegistrationMap map) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO tpp_organisation_gms_registration_map (organisation_id, patient_id, gms_organisation_id) VALUES (?, ?, ?) "
                    + " ON DUPLICATE KEY UPDATE"
                    + " organisation_id = VALUES(organisation_id),"
                    + " patient_id = VALUES(patient_id),"
                    + " gms_organisation_id = VALUES(gms_organisation_id)";

            ps = connection.prepareStatement(sql);
            ps.setString(1, map.getOrganisationId());
            ps.setInt(2, map.getPatientId());
            ps.setString(3, map.getGmsOrganisationId());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public List<TppOrganisationGmsRegistrationMap> getTppOrganisationGmsRegistrationMapsFromOrgId(String orgId) throws Exception {

        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT * FROM tpp_organisation_gms_registration_map WHERE organisation_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, orgId);

            List<TppOrganisationGmsRegistrationMap> ret = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                int col = 1;
                String organisationId = rs.getString(col++);
                Integer patientId = rs.getInt(col++);
                String gmsOrganisationId = rs.getString(col++);

                TppOrganisationGmsRegistrationMap m = new TppOrganisationGmsRegistrationMap();
                m.setOrganisationId(organisationId);
                m.setPatientId(patientId);
                m.setGmsOrganisationId(gmsOrganisationId);

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
    public ConfigurationLockI createConfigurationLock(String lockName) throws Exception {

        //the lock uses a single connection over a long period of time, so we want a connection
        //that's not managed by the connection pool
        Connection connection = ConnectionManager.getSftpReaderNonPooledConnection();
        return new MySqlConfigurationLock(lockName, connection);
    }

    @Override
    public List<String> getNotifiedMessages(BatchSplit batchSplit) throws Exception {

        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "select m.outbound "
                    + "from batch_split bs "
                    + "inner join notification_message m "
                    + "on m.configuration_id = bs.configuration_id "
                    + "and m.batch_id = bs.batch_id "
                    + "and m.batch_split_id = bs.batch_split_id "
                    + "where bs.configuration_id = ? "
                    + "and bs.local_relative_path = ? "
                    + "and m.was_success = true "
                    + "and bs.have_notified = true "
                    + "and bs.batch_split_id != ?";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, batchSplit.getConfigurationId());
            ps.setString(col++, batchSplit.getLocalRelativePath());
            ps.setInt(col++, batchSplit.getBatchSplitId());

            List<String> ret = new ArrayList<>();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String s = rs.getString(1);
                ret.add(s);
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
    public ConfigurationPollingAttempt getLastPollingAttempt(String configurationId) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT configuration_id, attempt_started, attempt_finished, exception_text, files_downloaded,"
                    + " batches_completed, batch_splits_notified_ok, batch_splits_notified_failure"
                    + " FROM configuration_polling_attempt"
                    + " WHERE configuration_id = ?"
                    + " ORDER BY attempt_started DESC"
                    + " LIMIT 1";

            ps = connection.prepareStatement(sql);

            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {

                int col = 1;

                ConfigurationPollingAttempt ret = new ConfigurationPollingAttempt();
                ret.setConfigurationId(rs.getString(col++));
                ret.setAttemptStarted(new Date(rs.getTimestamp(col++).getTime()));
                ret.setAttemptFinished(new Date(rs.getTimestamp(col++).getTime()));
                ret.setErrorText(rs.getString(col++));
                ret.setFilesDownloaded(rs.getInt(col++));
                ret.setBatchesCompleted(rs.getInt(col++));
                ret.setBatchSplitsNotifiedOk(rs.getInt(col++));
                ret.setBatchSplitsNotifiedFailure(rs.getInt(col++));
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
    public void savePollingAttempt(ConfigurationPollingAttempt attempt) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO configuration_polling_attempt"
            + " (configuration_id, attempt_started, attempt_finished, exception_text, files_downloaded,"
            + " batches_completed, batch_splits_notified_ok, batch_splits_notified_failure)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, attempt.getConfigurationId());
            ps.setTimestamp(col++, new Timestamp(attempt.getAttemptStarted().getTime()));
            ps.setTimestamp(col++, new Timestamp(attempt.getAttemptFinished().getTime()));
            if (attempt.hasError()) {
                ps.setString(col++, attempt.getErrorText());
            } else {
                ps.setNull(col++, Types.VARCHAR);
            }
            ps.setInt(col++, attempt.getFilesDownloaded());
            ps.setInt(col++, attempt.getBatchesCompleted());
            ps.setInt(col++, attempt.getBatchSplitsNotifiedOk());
            ps.setInt(col++, attempt.getBatchSplitsNotifiedFailure());

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }

    }

    @Override
    public Set<String> getAdastraOdsCodes(String configurationId, String fileNameOrgCode) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT ods_code"
                    + " FROM adastra_organisation_map"
                    + " WHERE configuration_id = ?"
                    + " AND file_name_org_code = ?";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, configurationId);
            ps.setString(col++, fileNameOrgCode);

            Set<String> ret = new HashSet<>();

            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String odsCode = rs.getString(1);
                ret.add(odsCode);
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
    public void saveAdastraOdsCode(String configurationId, String fileNameOrgCode, String odsCode) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO adastra_organisation_map"
                    + " (ods_code, file_name_org_code, configuration_id)"
                    + " VALUES (?, ?, ?)";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, odsCode);
            ps.setString(col++, fileNameOrgCode);
            ps.setString(col++, configurationId);

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public void resetBatch(int batchId) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {

            String sql = "DELETE FROM notification_message WHERE batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, batchId);
            ps.executeUpdate();
            ps.close();
            ps = null;

            sql = "DELETE FROM batch_split WHERE batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, batchId);
            ps.executeUpdate();
            ps.close();
            ps = null;

            sql = "UPDATE batch SET is_complete = false, complete_date = null WHERE batch_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setInt(1, batchId);
            ps.executeUpdate();
            ps.close();
            ps = null;

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    @Override
    public Date isPausedNotifyingMessagingApi(String configurationId) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {

            String sql = "SELECT dt_paused FROM configuration_paused_notifying WHERE configuration_id = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, configurationId);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new java.util.Date(rs.getTimestamp(1).getTime());

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
    public void addTppOrganisationMappings(List<TppOrganisationMap> mappings) throws Exception {
        Connection connection = getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO tpp_organisation_map (ods_code, name) VALUES (?, ?) "
                    + " ON DUPLICATE KEY UPDATE"
                    + " name = VALUES(name)";

            ps = connection.prepareStatement(sql);

            for (TppOrganisationMap mapping: mappings) {
                ps.setString(1, mapping.getOdsCode());
                ps.setString(2, mapping.getName());
                ps.addBatch();
            }

            ps.executeBatch();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }

    private Connection getConnection() throws Exception {
        Connection conn = ConnectionManager.getSftpReaderConnection();
        conn.setAutoCommit(true);
        return conn;
    }
}
