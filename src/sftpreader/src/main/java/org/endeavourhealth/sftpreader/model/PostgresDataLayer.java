package org.endeavourhealth.sftpreader.model;

import com.google.common.base.Strings;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.pool.ProxyConnection;
import org.endeavourhealth.common.postgres.PgDataSource;
import org.endeavourhealth.common.postgres.PgResultSet;
import org.endeavourhealth.common.postgres.PgStoredProc;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.postgres.logdigest.IDBDigestLogger;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.SftpFile;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class PostgresDataLayer implements DataLayerI, IDBDigestLogger {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(PostgresDataLayer.class);

    private String dbUrl;
    private String dbUsername;
    private String dbPassword;
    private String dbDriverClassName;
    private DataSource dataSource;

    public PostgresDataLayer(String dbUrl, String dbUsername, String dbPassword, String dbDriverClassName) throws Exception {
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

    public DbInstance getInstanceConfiguration(String instanceName, String hostname) throws PgStoredProcException {

        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.get_instance_configuration")
                .addParameter("_instance_name", instanceName)
                .addParameter("_hostname", hostname);

        DbInstance dbInstance = pgStoredProc.executeMultiQuerySingleRow((resultSet) ->
                new DbInstance()
                    .setInstanceName(resultSet.getString("instance_name"))
                    .setHttpManagementPort(PgResultSet.getInteger(resultSet, "http_management_port")));

        List<String> configurationIds = pgStoredProc.executeMultiQuery((resultSet) -> resultSet.getString("configuration_id"));

        DbInstanceEds dbInstanceEds = pgStoredProc.executeMultiQuerySingleOrEmptyRow((resultSet) ->
                new DbInstanceEds()
                        .setEdsUrl(resultSet.getString("eds_url"))
                        .setUseKeycloak(resultSet.getBoolean("use_keycloak"))
                        .setKeycloakTokenUri(resultSet.getString("keycloak_token_uri"))
                        .setKeycloakRealm(resultSet.getString("keycloak_realm"))
                        .setKeycloakUsername(resultSet.getString("keycloak_username"))
                        .setKeycloakPassword(resultSet.getString("keycloak_password"))
                        .setKeycloakClientId(resultSet.getString("keycloak_clientid"))
                        .setTempDirectory(resultSet.getString("temp_directory"))
                        .setSharedStoragePath(resultSet.getString("shared_storage_path")));

        return dbInstance
                .setConfigurationIds(configurationIds)
                .setEdsConfiguration(dbInstanceEds);

        /*DbInstanceSlack dbInstanceSlack = pgStoredProc.executeMultiQuerySingleRow((resultSet) ->
                new DbInstanceSlack()
                        .setEnabled(resultSet.getBoolean("slack_enabled"))
                        .setSlackUrl(resultSet.getString("slack_url")));

        return dbInstance
                .setConfigurationIds(configurationIds)
                .setEdsConfiguration(dbInstanceEds)
                .setSlackConfiguration(dbInstanceSlack);*/
    }

    public DbConfiguration getConfiguration(String configurationId) throws PgStoredProcException {

        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.get_configuration")
                .addParameter("_configuration_id", configurationId);

        DbConfiguration dbConfiguration = pgStoredProc.executeMultiQuerySingleOrEmptyRow((resultSet) ->
                new DbConfiguration()
                        .setConfigurationId(resultSet.getString("configuration_id"))
                        .setConfigurationFriendlyName(resultSet.getString("configuration_friendly_name"))
                        .setInterfaceTypeName(resultSet.getString("interface_type_name"))
                        .setPollFrequencySeconds(resultSet.getInt("poll_frequency_seconds"))
                        //.setLocalRootPathPrefix(resultSet.getString("local_root_path_prefix"))
                        .setLocalRootPath(resultSet.getString("local_root_path"))
                        .setSoftwareContentType(resultSet.getString("software_content_type"))
                        .setSoftwareVersion(resultSet.getString("software_version")));

        if (dbConfiguration == null)
            throw new PgStoredProcException("No configuration found with configuration id " + configurationId);

        DbConfigurationSftp dbConfigurationSftp = pgStoredProc.executeMultiQuerySingleOrEmptyRow((resultSet) ->
                new DbConfigurationSftp()
                        .setHostname(resultSet.getString("hostname"))
                        .setPort(resultSet.getInt("port"))
                        .setRemotePath(resultSet.getString("remote_path"))
                        .setUsername(resultSet.getString("username"))
                        .setClientPrivateKey(resultSet.getString("client_private_key"))
                        .setClientPrivateKeyPassword(resultSet.getString("client_private_key_password"))
                        .setHostPublicKey(resultSet.getString("host_public_key")));

        if (dbConfigurationSftp == null)
            throw new PgStoredProcException("No SFTP configuration details found for configuration id " + configurationId);

        DbConfigurationPgp dbConfigurationPgp = pgStoredProc.executeMultiQuerySingleOrEmptyRow((resultSet) ->
                new DbConfigurationPgp()
                        .setPgpFileExtensionFilter(resultSet.getString("pgp_file_extension_filter"))
                        .setPgpSenderPublicKey(resultSet.getString("pgp_sender_public_key"))
                        .setPgpRecipientPublicKey(resultSet.getString("pgp_recipient_public_key"))
                        .setPgpRecipientPrivateKey(resultSet.getString("pgp_recipient_private_key"))
                        .setPgpRecipientPrivateKeyPassword(resultSet.getString("pgp_recipient_private_key_password")));

        //No Pgp needed for non EMIS
//        if (dbConfigurationPgp == null)
//            throw new PgStoredProcException("No PGP configuration details found for configuration id " + configurationId);

        List<DbConfigurationKvp> dbConfigurationKvp = pgStoredProc.executeMultiQuery((resultSet) ->
                new DbConfigurationKvp()
                        .setKey(resultSet.getString("key"))
                        .setValue(resultSet.getString("value")));

        List<String> interfaceFileTypes = pgStoredProc.executeMultiQuery((resultSet) ->
            resultSet.getString("file_type_identifier"));

        // assemble data

        if (dbConfiguration == null)
            return null;

        return dbConfiguration
                .setSftpConfiguration(dbConfigurationSftp)
                .setPgpConfiguration(dbConfigurationPgp)
                .setDbConfigurationKvp(dbConfigurationKvp)
                .setInterfaceFileTypes(interfaceFileTypes);
    }

    public void addEmisOrganisationMap(EmisOrganisationMap mapping) throws PgStoredProcException {

        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.add_emis_organisation_map")
                .addParameter("_guid", mapping.getGuid())
                .addParameter("_name", mapping.getName())
                .addParameter("_ods_code", mapping.getOdsCode());

        pgStoredProc.execute();
    }

    public EmisOrganisationMap getEmisOrganisationMap(String guid) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.get_emis_organisation_map")
                .addParameter("_guid", guid);

        List<EmisOrganisationMap> mappings = pgStoredProc.executeQuery(resultSet -> new EmisOrganisationMap()
                .setGuid(resultSet.getString("guid"))
                .setName(resultSet.getString("name"))
                .setOdsCode(resultSet.getString("ods_code")));

        if (mappings.isEmpty())
            return null;

        return mappings.get(0);
    }

    public AddFileResult addFile(String configurationId, SftpFile sftpFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_file")
                .addParameter("_configuration_id", configurationId)
                .addParameter("_batch_identifier", sftpFile.getBatchIdentifier())
                .addParameter("_file_type_identifier", sftpFile.getFileTypeIdentifier())
                .addParameter("_filename", sftpFile.getFilename())
                .addParameter("_local_relative_path", sftpFile.getLocalRelativePath())
                .addParameter("_remote_size_bytes", sftpFile.getRemoteFileSizeInBytes())
                .addParameter("_remote_created_date", sftpFile.getRemoteLastModifiedDate());
                //.addParameter("_requires_decryption", sftpFile.doesFileNeedDecrypting());

        return pgStoredProc.executeSingleRow((resultSet) ->
                new AddFileResult()
                    .setFileAlreadyDownloaded(resultSet.getBoolean("file_already_processed"))
                    .setBatchFileId(resultSet.getInt("batch_file_id")));
    }

    public void setFileAsDownloaded(SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_file_as_downloaded")
                .addParameter("_batch_file_id", batchFile.getBatchFileId());
                //.addParameter("_local_size_bytes", batchFile.getLocalFileSizeBytes());

        pgStoredProc.execute();
    }

    @Override
    public void setFileAsDeleted(BatchFile batchFile) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "UPDATE log.batch_file SET is_deleted = ? WHERE batch_file_id = ?;";

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

    /*public void setFileAsDecrypted(BatchFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_file_as_decrypted")
                .addParameter("_batch_file_id", batchFile.getBatchFileId())
                .addParameter("_decrypted_filename", batchFile.getDecryptedFilename())
                .addParameter("_decrypted_size_bytes", batchFile.getDecryptedSizeBytes());

        pgStoredProc.execute();
    }*/

    public boolean addUnknownFile(String configurationId, SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_unknown_file")
                .addParameter("_configuration_id", configurationId)
                .addParameter("_filename", batchFile.getFilename())
                .addParameter("_remote_size_bytes", batchFile.getRemoteFileSizeInBytes())
                .addParameter("_remote_created_date", batchFile.getRemoteLastModifiedDate());

        return pgStoredProc.executeSingleRow(resultSet -> resultSet.getBoolean(1));
        //return pgStoredProc.executeMultiQuerySingleRow(resultSet -> resultSet.getBoolean(1));
        //pgStoredProc.execute();
    }

    public List<Batch> getIncompleteBatches(String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_incomplete_batches")
                .addParameter("_configuration_id", configurationId);

        return populateBatches(pgStoredProc);
    }

    public Batch getLastCompleteBatch(String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_last_complete_batch")
                .addParameter("_configuration_id", configurationId);

        List<Batch> batches = populateBatches(pgStoredProc);

        if (batches.size() > 1)
            throw new PgStoredProcException("More than one last complete batch returned");

        if (batches.size() == 0)
            return null;

        return batches.get(0);
    }

    @Override
    public List<Batch> getAllBatches(String configurationId) throws Exception {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_all_batches")
                .addParameter("_configuration_id", configurationId);

        return populateBatches(pgStoredProc);
    }

    public List<BatchSplit> getUnnotifiedBatchSplits(String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_unnotified_batch_splits")
                .addParameter("_configuration_id", configurationId);

        return populateBatchSplits(pgStoredProc, configurationId);
    }

    private static List<BatchSplit> populateBatchSplits(PgStoredProc pgStoredProc, String configurationId) throws PgStoredProcException {
        List<BatchSplit> batchSplits = pgStoredProc.executeMultiQuery(resultSet ->
                new BatchSplit()
                    .setBatchSplitId(resultSet.getInt("batch_split_id"))
                    .setBatchId(resultSet.getInt("batch_id"))
                    .setLocalRelativePath(resultSet.getString("local_relative_path"))
                    .setOrganisationId(resultSet.getString("organisation_id"))
                    .setConfigurationId(configurationId));

        List<Batch> batches = populateBatches(pgStoredProc);

        for (Batch batch: batches)
            for (BatchSplit batchSplit: batchSplits)
                if (batchSplit.getBatchId() == batch.getBatchId())
                    batchSplit.setBatch(batch);

        return batchSplits;
    }

    public List<UnknownFile> getUnknownFiles(String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_unknown_files")
                .addParameter("_configuration_id", configurationId);

        return pgStoredProc.executeQuery(resultSet -> new UnknownFile()
                .setUnknownFileId(resultSet.getInt("unknown_file_id"))
                .setFilename(resultSet.getString("filename"))
                .setInsertDate(PgResultSet.getLocalDateTime(resultSet, "insert_date"))
                .setRemoteCreatedDate(PgResultSet.getLocalDateTime(resultSet, "remote_created_date"))
                .setRemoteSizeBytes(resultSet.getLong("remote_size_bytes")));
    }

    private static List<Batch> populateBatches(PgStoredProc pgStoredProc) throws PgStoredProcException {
        List<Batch> batches = pgStoredProc.executeMultiQuery(resultSet ->
                new Batch()
                        .setBatchId(resultSet.getInt("batch_id"))
                        .setBatchIdentifier(resultSet.getString("batch_identifier"))
                        .setLocalRelativePath(resultSet.getString("local_relative_path"))
                        .setInsertDate(getDate(resultSet, "insert_date"))
                        .setSequenceNumber(PgResultSet.getInteger(resultSet, "sequence_number"))
                        .setCompleteDate(getDate(resultSet, "complete_date")));

        List<BatchFile> batchFiles = pgStoredProc.executeMultiQuery(resultSet ->
                new BatchFile()
                        .setBatchId(resultSet.getInt("batch_id"))
                        .setBatchFileId(resultSet.getInt("batch_file_id"))
                        .setFileTypeIdentifier(resultSet.getString("file_type_identifier"))
                        .setFilename(resultSet.getString("filename"))
                        .setRemoteSizeBytes(resultSet.getLong("remote_size_bytes"))
                        .setDownloaded(resultSet.getBoolean("is_downloaded"))
                        .setDeleted(resultSet.getBoolean("is_deleted"))
        );

        batchFiles.forEach(t ->
                batches
                        .stream()
                        .filter(s -> s.getBatchId() == t.getBatchId())
                        .collect(StreamExtension.singleCollector())
                        .addBatchFile(t));

        return batches;
    }

    public void setBatchAsComplete(Batch batch) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_batch_as_complete")
                .addParameter("_batch_id", batch.getBatchId());

        pgStoredProc.execute();
    }

    public void setBatchSequenceNumber(Batch batch, Integer sequenceNumber) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_batch_sequence_number")
                .addParameter("_batch_id", batch.getBatchId());

        //seq number may be null if we're clearing it
        if (sequenceNumber == null) {
            pgStoredProc.addParameter("_sequence_number", null);
        } else {
            pgStoredProc.addParameter("_sequence_number", Integer.toString(sequenceNumber));
        }

        pgStoredProc.execute();
    }

    public void addBatchNotification(int batchId, int batchSplitId, String configurationId, UUID messageId, String outboundMessage, String inboundMessage, boolean wasSuccess, String errorText) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_batch_notification")
                .addParameter("_batch_id", batchId)
                .addParameter("_batch_split_id", batchSplitId)
                .addParameter("_configuration_id", configurationId)
                .addParameter("_message_uuid", messageId)
                .addParameter("_outbound_message", outboundMessage)
                .addParameter("_inbound_message", inboundMessage)
                .addParameter("_was_success", wasSuccess)
                .addParameter("_error_text", errorText);

        pgStoredProc.execute();
    }

    public void addBatchSplit(BatchSplit batchSplit) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_batch_split")
                .addParameter("_batch_id", batchSplit.getBatchId())
                .addParameter("_configuration_id", batchSplit.getConfigurationId())
                .addParameter("_local_relative_path", batchSplit.getLocalRelativePath())
                .addParameter("_organisation_id", batchSplit.getOrganisationId());

        pgStoredProc.execute();
    }

    public void deleteBatchSplits(Batch batch) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.delete_batch_splits")
                .addParameter("_batch_id", batch.getBatchId());

        pgStoredProc.execute();
    }

    public void logErrorDigest(String logClass, String logMethod, String logMessage, String exception) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.log_error_digest")
                .addParameter("_log_class", logClass)
                .addParameter("_log_method", logMethod)
                .addParameter("_log_message", logMessage)
                .addParameter("_exception", exception);

        pgStoredProc.execute();
    }

    /*
    * quick and dirty function to get the name for an org ODS code
    **/
    public List<EmisOrganisationMap> getEmisOrganisationMapsForOdsCode(String odsCode) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select guid, name from configuration.emis_organisation_map where ods_code = '" + odsCode + "';");

            List<EmisOrganisationMap> ret = new ArrayList<>();

            while (rs.next()) {
                int col = 1;
                String guid = rs.getString(col++);
                String name = rs.getString(col++);

                EmisOrganisationMap m = new EmisOrganisationMap();
                m.setGuid(guid);
                m.setName(name);
                m.setOdsCode(odsCode);
                ret.add(m);
            }

            return ret;

        } catch (Exception ex) {
            LOG.error("Error getting name for ODS code " + odsCode, ex);
            return null;
        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    LOG.error("Error closing connection", se);
                }
            }
        }
    }

    private static Date getDate(ResultSet resultSet, String columnName) throws SQLException {
        Date d = resultSet.getDate(columnName);
        if (resultSet.wasNull()) {
            return null;
        } else {
            return d;
        }
    }

    public List<BatchSplit> getBatchSplitsForBatch(int queryBatchId) throws Exception {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from log.batch_split where batch_id = " + queryBatchId + ";");

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

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    LOG.error("Error closing connection", se);
                }
            }
        }
    }

    public void addTppOrganisationMap(TppOrganisationMap mapping) throws PgStoredProcException {

        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.add_tpp_organisation_map")
                .addParameter("_ods_code", mapping.getOdsCode())
                .addParameter("_name", mapping.getName());

        pgStoredProc.execute();
    }

    @Override
    public TppOrganisationMap getTppOrgNameFromOdsCode(String queryOdsCode) throws Exception {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select * from configuration.tpp_organisation_map where ods_code = '" + queryOdsCode + "';");

            if (rs.next()) {
                int col = 1;
                String odsCode = rs.getString(col++);
                String name = rs.getString(col++);

                TppOrganisationMap ret = new TppOrganisationMap();
                ret.setOdsCode(odsCode);
                ret.setName(name);

                return ret;

            } else {
                return null;
            }

        } finally {

            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException se) {
                    LOG.error("Error closing connection", se);
                }
            }
        }
    }

    @Override
    public ConfigurationLockI createConfigurationLock(String lockName) throws Exception {

        //the lock uses a single connection over a long period of time, so we want a connection
        //that's not managed by the connection pool
        DataSource nonPooledSource = PgDataSource.get(dbUrl, dbUsername, dbPassword);
        Connection connection = nonPooledSource.getConnection();

        return new PostgresConfigurationLock(lockName, connection);
    }

    @Override
    public List<String> getNotifiedMessages(BatchSplit batchSplit) throws Exception {

        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "select m.outbound "
                    + "from log.batch_split bs "
                    + "inner join log.notification_message m "
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
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT configuration_id, attempt_started, attempt_finished, exception_text, files_downloaded,"
                    + " batches_completed, batch_splits_notified_ok, batch_splits_notified_failure"
                    + " FROM log.configuration_polling_attempt"
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
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO log.configuration_polling_attempt"
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
    public Set<String> getAdastraOdsCodes(String configurationId) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "SELECT ods_code"
                    + " FROM configuration.adastra_organisation_map"
                    + " WHERE configuration_id = ?";

            ps = connection.prepareStatement(sql);

            ps.setString(1, configurationId);

            Set<String> ret = new HashSet<>();

            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
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
    public void saveAdastraOdsCode(String configurationId, String odsCode) throws Exception {
        Connection connection = dataSource.getConnection();
        PreparedStatement ps = null;
        try {
            String sql = "INSERT INTO configuration.adastra_organisation_map"
                    + " (ods_code, configuration_id)"
                    + " VALUES (?, ?)";

            ps = connection.prepareStatement(sql);

            int col = 1;
            ps.setString(col++, odsCode);
            ps.setString(col++, configurationId);

            ps.executeUpdate();

        } finally {
            if (ps != null) {
                ps.close();
            }
            connection.close();
        }
    }
}
