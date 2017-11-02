package org.endeavourhealth.sftpreader;

import org.endeavourhealth.common.postgres.PgResultSet;
import org.endeavourhealth.common.postgres.PgStoredProc;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.postgres.logdigest.IDBDigestLogger;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class DataLayer implements IDBDigestLogger {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(DataLayer.class);

    private DataSource dataSource;

    public DataLayer(DataSource dataSource) {
        this.dataSource = dataSource;
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
                        /*.setSoftwareContentType(resultSet.getString("software_content_type"))
                        .setSoftwareVersion(resultSet.getString("software_version"))*/
                        .setUseKeycloak(resultSet.getBoolean("use_keycloak"))
                        .setKeycloakTokenUri(resultSet.getString("keycloak_token_uri"))
                        .setKeycloakRealm(resultSet.getString("keycloak_realm"))
                        .setKeycloakUsername(resultSet.getString("keycloak_username"))
                        .setKeycloakPassword(resultSet.getString("keycloak_password"))
                        .setKeycloakClientId(resultSet.getString("keycloak_clientid")));

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
                        .setLocalRootPathPrefix(resultSet.getString("local_root_path_prefix"))
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

        if (dbConfigurationPgp == null)
            throw new PgStoredProcException("No PGP configuration details found for configuration id " + configurationId);

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

        LocalDate localDate = null;
        if (mapping.getStartDate() != null) {
            java.util.Date d = mapping.getStartDate();
            localDate = d.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        }

        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.add_emis_organisation_map")
                .addParameter("_guid", mapping.getGuid())
                .addParameter("_name", mapping.getName())
                .addParameter("_ods_code", mapping.getOdsCode())
                .addParameter("_start_date", localDate);

        pgStoredProc.execute();
    }

    public EmisOrganisationMap getEmisOrganisationMap(String guid) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("configuration.get_emis_organisation_map")
                .addParameter("_guid", guid);

        List<EmisOrganisationMap> mappings = pgStoredProc.executeQuery(resultSet -> new EmisOrganisationMap()
                .setGuid(resultSet.getString("guid"))
                .setName(resultSet.getString("name"))
                .setOdsCode(resultSet.getString("ods_code"))
                .setStartDate(resultSet.getDate("start_date")));

        if (mappings.isEmpty())
            return null;

        return mappings.get(0);
    }

    public AddFileResult addFile(String configurationId, SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_file")
                .addParameter("_configuration_id", configurationId)
                .addParameter("_batch_identifier", batchFile.getBatchIdentifier())
                .addParameter("_file_type_identifier", batchFile.getFileTypeIdentifier())
                .addParameter("_filename", batchFile.getFilename())
                .addParameter("_local_relative_path", batchFile.getLocalRelativePath())
                .addParameter("_remote_size_bytes", batchFile.getRemoteFileSizeInBytes())
                .addParameter("_remote_created_date", batchFile.getRemoteLastModifiedDate())
                .addParameter("_requires_decryption", batchFile.doesFileNeedDecrypting());

        return pgStoredProc.executeSingleRow((resultSet) ->
                new AddFileResult()
                    .setFileAlreadyProcessed(resultSet.getBoolean("file_already_processed"))
                    .setBatchFileId(resultSet.getInt("batch_file_id")));
    }

    public void setFileAsDownloaded(SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_file_as_downloaded")
                .addParameter("_batch_file_id", batchFile.getBatchFileId())
                .addParameter("_local_size_bytes", batchFile.getLocalFileSizeBytes());

        pgStoredProc.execute();
    }

    public void setFileAsDecrypted(SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_file_as_decrypted")
                .addParameter("_batch_file_id", batchFile.getBatchFileId())
                .addParameter("_decrypted_filename", batchFile.getDecryptedFilename())
                .addParameter("_decrypted_size_bytes", batchFile.getDecryptedFileSizeBytes());

        pgStoredProc.execute();
    }

    public void addUnknownFile(String configurationId, SftpFile batchFile) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_unknown_file")
                .addParameter("_configuration_id", configurationId)
                .addParameter("_filename", batchFile.getFilename())
                .addParameter("_remote_size_bytes", batchFile.getRemoteFileSizeInBytes())
                .addParameter("_remote_created_date", batchFile.getRemoteLastModifiedDate());

        pgStoredProc.execute();
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

    public List<BatchSplit> getUnnotifiedBatchSplits(String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.get_unnotified_batch_splits")
                .addParameter("_configuration_id", configurationId);

        return populateBatchSplits(pgStoredProc);
    }

    private static List<BatchSplit> populateBatchSplits(PgStoredProc pgStoredProc) throws PgStoredProcException {
        List<BatchSplit> batchSplits = pgStoredProc.executeMultiQuery(resultSet ->
                new BatchSplit()
                    .setBatchSplitId(resultSet.getInt("batch_split_id"))
                    .setBatchId(resultSet.getInt("batch_id"))
                    .setLocalRelativePath(resultSet.getString("local_relative_path"))
                    .setOrganisationId(resultSet.getString("organisation_id")));

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
                        .setSequenceNumber(PgResultSet.getInteger(resultSet, "sequence_number")));

        List<BatchFile> batchFiles = pgStoredProc.executeMultiQuery(resultSet ->
                new BatchFile()
                        .setBatchId(resultSet.getInt("batch_id"))
                        .setBatchFileId(resultSet.getInt("batch_file_id"))
                        .setFileTypeIdentifier(resultSet.getString("file_type_identifier"))
                        .setFilename(resultSet.getString("filename"))
                        .setRemoteSizeBytes(resultSet.getLong("remote_size_bytes"))
                        .setDownloaded(resultSet.getBoolean("is_downloaded"))
                        .setLocalSizeBytes(resultSet.getLong("local_size_bytes"))
                        .setRequiresDecryption(resultSet.getBoolean("requires_decryption"))
                        .setDecrypted(resultSet.getBoolean("is_decrypted"))
                        .setDecryptedFilename(resultSet.getString("decrypted_filename"))
                        .setDecryptedSizeBytes(resultSet.getLong("decrypted_size_bytes")));

        batchFiles.forEach(t ->
                batches
                        .stream()
                        .filter(s -> s.getBatchId() == t.getBatchId())
                        .collect(StreamExtension.singleCollector())
                        .addBatchFile(t));

        return batches;
    }

    public void setBatchAsComplete(Batch batch, int sequenceNumber) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.set_batch_as_complete")
                .addParameter("_batch_id", batch.getBatchId())
                .addParameter("_sequence_number", Integer.toString(sequenceNumber));

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

    public void addBatchSplit(BatchSplit batchSplit, String configurationId) throws PgStoredProcException {
        PgStoredProc pgStoredProc = new PgStoredProc(dataSource)
                .setName("log.add_batch_split")
                .addParameter("_batch_id", batchSplit.getBatchId())
                .addParameter("_configuration_id", configurationId)
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
    public String findEmisOrgNameFromOdsCode(String odsCode) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select name from configuration.emis_organisation_map where ods_code = '" + odsCode + "';");

            String ret = null;

            //we have multiple names for some orgs in production (e.g. F84636),
            //so return the name with the longest length (for the sake of having some way to choose
            //something more interesting that just "The Surgery")
            while (rs.next()) {
                String s = rs.getString(1);
                if (ret == null
                        || s.length() > ret.length()) {
                    ret = s;
                }
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

    /*
    * runs sql to get the start date for an emis practice
    **/
    public Date findEmisOrgStartDateFromOdsCode(String odsCode) {
        Connection connection = null;

        try {
            connection = dataSource.getConnection();

            Statement statement = connection.createStatement();
            ResultSet rs = statement.executeQuery("select start_date from configuration.emis_organisation_map where ods_code = '" + odsCode + "';");

            Date ret = null;

            //we have multiple names for some orgs in production (e.g. F84636),
            //so return any non-null date we find
            while (rs.next()) {
                java.sql.Date d = rs.getDate(1);
                if (d != null) {
                    //create a java util date from the sql date, since sql Date doesn't support the toInstant function
                    ret = new java.util.Date(d.getTime());
                }
            }

            return ret;

        } catch (Exception ex) {
            LOG.error("Error getting start date for ODS code " + odsCode, ex);
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
}
