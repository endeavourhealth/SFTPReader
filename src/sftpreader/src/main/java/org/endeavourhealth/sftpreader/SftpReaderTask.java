package org.endeavourhealth.sftpreader;

import com.google.common.base.Strings;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
//import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.eds.EdsSender;
import org.endeavourhealth.common.eds.EdsSenderHttpErrorResponseException;
import org.endeavourhealth.common.eds.EdsSenderResponse;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.implementations.*;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.*;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnection;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnectionException;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

public class SftpReaderTask implements Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpReaderTask.class);

    private static Map<Integer, String> notificationErrorrs = new HashMap<>();


    private Configuration configuration = null;
    private String configurationId = null;
    private DbInstance dbInstanceConfiguration = null;
    private DbConfiguration dbConfiguration = null;
    private DataLayer db = null;

    public SftpReaderTask(Configuration configuration, String configurationId) {
        this.configuration = configuration;
        this.configurationId = configurationId;
    }

    @Override
    public void run() {
        try {
            LOG.trace(">>>Starting scheduled SftpReader run, initialising");
            initialise();

            LOG.trace(">>>Downloading and decrypting files");
            if (!downloadAndProcessFiles()) {
                throw new SftpReaderException("Exception occurred downloading and processing files - halting to prevent incorrect ordering of batches.");
            }

            LOG.trace(">>>Validating and sequencing batches");
            if (!validateAndSequenceBatches()) {
                throw new SftpReaderException("Exception occurred validating and sequencing batches - halting to prevent incorrect ordering of batches.");
            }

            LOG.trace(">>>Notifying EDS");
            notifyEds();

            LOG.trace(">>>Completed SftpReader run");

        } catch (Exception e) {
            LOG.error(">>>Fatal exception in SftpTask run, terminating this run", e);
        }
    }

    private void initialise() throws Exception {
        this.dbInstanceConfiguration = configuration.getInstanceConfiguration();
        this.dbConfiguration = configuration.getConfiguration(configurationId);
        this.db = new DataLayer(configuration.getDatabaseConnection());
        checkLocalRootPathPrefixExists();
    }

    private void checkLocalRootPathPrefixExists() throws Exception {
        if (StringUtils.isNotEmpty(this.dbConfiguration.getLocalRootPathPrefix())) {

            File rootPath = new File(this.dbConfiguration.getLocalRootPathPrefix());

            if ((!rootPath.exists()) || (!rootPath.isDirectory()))
                throw new SftpReaderException("Local root path prefix '" + rootPath + "' does not exist");
        }
    }

    private boolean downloadAndProcessFiles() {
        Connection connection = null;

        try {
            connection = openSftpConnection(dbConfiguration.getSftpConfiguration());

            String remotePath = dbConfiguration.getSftpConfiguration().getRemotePath();

            List<RemoteFile> remoteFiles = getFileList(connection, remotePath);

            int countAlreadyProcessed = 0;

            LOG.trace("Found {} files in {}", new Integer(remoteFiles.size()), remotePath);

            for (RemoteFile remoteFile : remoteFiles) {
                SftpFile batchFile = instantiateSftpBatchFile(remoteFile);

                if (!batchFile.isFilenameValid()) {
                    LOG.error("   Invalid filename, skipping: " + batchFile.getFilename());
                    db.addUnknownFile(dbConfiguration.getConfigurationId(), batchFile);
                    continue;
                }

                AddFileResult addFileResult = db.addFile(configurationId, batchFile);

                if (addFileResult.isFileAlreadyProcessed()) {
                    countAlreadyProcessed ++;
                    continue;
                }

                batchFile.setBatchFileId(addFileResult.getBatchFileId());

                createBatchDirectory(batchFile);

                downloadFile(connection, batchFile);

                if (batchFile.doesFileNeedDecrypting())
                    decryptFile(batchFile);
            }

            if (countAlreadyProcessed > 0)
                LOG.trace("Skipped {} files as already processed them", new Integer(countAlreadyProcessed));

            LOG.info("Completed processing {} files", Integer.toString(remoteFiles.size()));

            return true;
        } catch (Exception e) {
            LOG.error("Exception occurred while processing files - cannot continue or may process batches out of order", e);
        } finally {
            closeConnection(connection);
        }

        return false;
    }

    private Connection openSftpConnection(DbConfigurationSftp configurationSftp) throws Exception, JSchException, IOException {
        Connection connection = ConnectionActivator.createConnection(dbConfiguration.getInterfaceTypeName(), getSftpConnectionDetails(configurationSftp));

        String hostname = connection.getConnectionDetails().getHostname();
        String port = Integer.toString(connection.getConnectionDetails().getPort());
        String username = connection.getConnectionDetails().getUsername();

        LOG.info(" Opening connection to " + hostname + " on port " + port + " with user " + username);

        connection.open();

        return connection;
    }

    private static ConnectionDetails getSftpConnectionDetails(DbConfigurationSftp configurationSftp) {
        return new ConnectionDetails()
                .setHostname(configurationSftp.getHostname())
                .setPort(configurationSftp.getPort())
                .setUsername(configurationSftp.getUsername())
                .setClientPrivateKey(configurationSftp.getClientPrivateKey())
                .setClientPrivateKeyPassword(configurationSftp.getClientPrivateKeyPassword())
                .setHostPublicKey(configurationSftp.getHostPublicKey());
    }

    private static void closeConnection(Connection connection) {
        if (connection != null)
            connection.close();
    }

    private static List<RemoteFile> getFileList(Connection connection, String remotePath) throws Exception {
        return connection.getFileList(remotePath);
    }

    private void downloadFile(Connection connection, SftpFile batchFile) throws Exception {
        String localFilePath = batchFile.getLocalFilePath();
        LOG.info("Downloading file to: " + localFilePath);
        File temporaryDownloadFile = new File(localFilePath + ".download");

        if (temporaryDownloadFile.exists()) {
            if (!temporaryDownloadFile.delete()) {
                throw new IOException("Could not delete existing temporary download file " + temporaryDownloadFile);
            }
        }

        String remoteFilePath = batchFile.getRemoteFilePath();

        InputStream inputStream = connection.getFile(remoteFilePath);
        Files.copy(inputStream, temporaryDownloadFile.toPath());

        //if we previously failure during decryption, the renamed file will already exist, so delete it
        File destination = new File(localFilePath);

        if (destination.exists()) {
            destination.delete();
        }

        if (!temporaryDownloadFile.renameTo(destination)) {
            throw new IOException("Could not temporary download file to " + localFilePath);
        }

        batchFile.setLocalFileSizeBytes(getFileSizeBytes(batchFile.getLocalFilePath()));

        db.setFileAsDownloaded(batchFile);
    }

    private SftpFile instantiateSftpBatchFile(RemoteFile remoteFile) {
        SftpFilenameParser sftpFilenameParser = ImplementationActivator.createFilenameParser(remoteFile.getFilename(), dbConfiguration, dbConfiguration.getInterfaceTypeName());

        return new SftpFile(remoteFile,
                sftpFilenameParser,
                dbConfiguration.getFullLocalRootPath());
    }

    private void createBatchDirectory(SftpFile batchFile) throws IOException {
        File localPath = new File(batchFile.getLocalPath());

        if (!localPath.exists())
            if (!localPath.mkdirs())
                throw new IOException("Could not create path " + localPath);
    }

    private void deleteRemoteFile(SftpConnection sftpConnection, String remoteFilePath) throws SftpException {
        LOG.info("Deleting remote file " + remoteFilePath);

        sftpConnection.deleteFile(remoteFilePath);
    }

    private void decryptFile(SftpFile batchFile) throws Exception {
        String localFilePath = batchFile.getLocalFilePath();
        String decryptedLocalFilePath = batchFile.getDecryptedLocalFilePath();
        String privateKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKey();
        String privateKeyPassword = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();
        //String publicKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPublicKey();
        String publicKey = dbConfiguration.getPgpConfiguration().getPgpSenderPublicKey();

        LOG.info("   Decrypting file to: " + decryptedLocalFilePath);

        PgpUtil.decryptAndVerify(localFilePath, privateKey, privateKeyPassword, decryptedLocalFilePath, publicKey);

        batchFile.setDecryptedFileSizeBytes(getFileSizeBytes(batchFile.getDecryptedLocalFilePath()));

        db.setFileAsDecrypted(batchFile);
    }

    private static long getFileSizeBytes(String filePath) {
        File file = new File(filePath);
        return file.length();
    }

    private boolean validateAndSequenceBatches() {
        try {
            List<UnknownFile> unknownFiles = getUnknownFiles();

            if (unknownFiles.size() > 0)
                throw new SftpValidationException("There are " + Integer.toString(unknownFiles.size()) + " unknown files present.");

            List<Batch> incompleteBatches = getIncompleteBatches();

            if (incompleteBatches.size() > 0) {
                Batch lastCompleteBatch = db.getLastCompleteBatch(dbConfiguration.getConfigurationId());

                validateBatches(incompleteBatches, lastCompleteBatch);
                splitBatches(incompleteBatches);
                List<Batch> sequencedBatches = sequenceBatches(incompleteBatches, lastCompleteBatch);

                SlackNotifier slackNotifier = new SlackNotifier(configuration);
                slackNotifier.notifyCompleteBatches(dbConfiguration, sequencedBatches);
            }

            return true;
        } catch (Exception e) {
            LOG.error("Error occurred during validation and sequencing", e);
        }

        return false;
    }

    private List<UnknownFile> getUnknownFiles() throws PgStoredProcException {
        return db.getUnknownFiles(dbConfiguration.getConfigurationId());
    }

    private List<Batch> getIncompleteBatches() throws PgStoredProcException {
        LOG.trace(" Getting batches ready for validation and sequencing");

        List<Batch> incompleteBatches = db.getIncompleteBatches(dbConfiguration.getConfigurationId());

        LOG.trace(" There are {} batches ready for validation and sequencing", Integer.toString(incompleteBatches.size()));

        return incompleteBatches;
    }

    private void validateBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch) throws SftpValidationException {
        String batchIdentifiers = StringUtils.join(incompleteBatches
                .stream()
                .map(t -> t.getBatchIdentifier())
                .collect(Collectors.toList()), ", ");

        LOG.trace(" Validating batches: " + batchIdentifiers);

        SftpBatchValidator sftpBatchValidator = ImplementationActivator.createSftpBatchValidator(dbConfiguration.getInterfaceTypeName());
        sftpBatchValidator.validateBatches(incompleteBatches, lastCompleteBatch, dbConfiguration, db);

        LOG.trace(" Completed batch validation");
    }

    private List<Batch> sequenceBatches(List<Batch> incompleteBatches, Batch lastCompleteBatch) throws SftpValidationException, SftpFilenameParseException, PgStoredProcException {
        LOG.trace(" Sequencing batches");

        int nextSequenceNumber = getNextSequenceNumber(lastCompleteBatch);

        SftpBatchSequencer sftpBatchSequencer = ImplementationActivator.createSftpBatchSequencer(dbConfiguration.getInterfaceTypeName());
        Map<Batch, Integer> batchSequence = sftpBatchSequencer.determineBatchSequenceNumbers(incompleteBatches, nextSequenceNumber, lastCompleteBatch);

        Map<Batch, Integer> sortedBatchSequence = StreamExtension.sortByValue(batchSequence);

        if (!new HashSet<>(incompleteBatches).equals(sortedBatchSequence.keySet()))
            throw new SftpValidationException("Batch sequence does not contain all unsequenced batches");

        for (Batch batch : sortedBatchSequence.keySet())
            if (sortedBatchSequence.get(batch).intValue() != nextSequenceNumber++)
                throw new SftpValidationException("Unexpected batch sequence number");

        for (Batch batch : sortedBatchSequence.keySet()) {
            LOG.debug("  Batch " + batch.getBatchIdentifier() + " sequenced as " + sortedBatchSequence.get(batch).toString());
            db.setBatchAsComplete(batch, sortedBatchSequence.get(batch));
        }

        LOG.trace(" Completed batch sequencing");

        return new ArrayList<>(sortedBatchSequence.keySet());
    }

    private void splitBatches(List<Batch> batches) throws Exception {
        LOG.trace("Splitting batches");

        for (Batch batch: batches) {

            //delete any pre-existing splits for this batch
            db.deleteBatchSplits(batch);

            SftpBatchSplitter sftpBatchSplitter = ImplementationActivator.createSftpBatchSplitter(dbConfiguration.getInterfaceTypeName());
            List<BatchSplit> splitBatches = sftpBatchSplitter.splitBatch(batch, db, dbConfiguration);

            for (BatchSplit splitBatch: splitBatches)
                db.addBatchSplit(splitBatch, dbConfiguration.getConfigurationId());
        }

        LOG.trace("Completed splitting");
    }

    private static int getNextSequenceNumber(Batch lastCompleteBatch) {
        if (lastCompleteBatch == null)
            return 1;

        return lastCompleteBatch.getSequenceNumber() + 1;
    }

    private void notifyEds() throws PgStoredProcException, SftpReaderException {

        List<BatchSplit> unnotifiedBatchSplits = db.getUnnotifiedBatchSplits(dbConfiguration.getConfigurationId());
        LOG.debug("There are {} complete split batches for notification", unnotifiedBatchSplits.size());

        if (unnotifiedBatchSplits.isEmpty()) {
            return;
        }

        DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();

        if (edsConfiguration == null)
            throw new SftpReaderException("Cannot notify EDS - EDS configuration is not set");

        if (edsConfiguration.isUseKeycloak()) {
            LOG.trace("Initialising keycloak at: {}", edsConfiguration.getKeycloakTokenUri());

            KeycloakClient.init(edsConfiguration.getKeycloakTokenUri(),
                    edsConfiguration.getKeycloakRealm(),
                    edsConfiguration.getKeycloakUsername(),
                    edsConfiguration.getKeycloakPassword(),
                    edsConfiguration.getKeycloakClientId());

            try {
                Header response = KeycloakClient.instance().getAuthorizationHeader();

                LOG.trace("Keycloak authorization header is {}: {}", response.getName(), response.getValue());
            } catch (IOException e) {
                throw new SftpReaderException("Error initialising keycloak", e);
            }
        }
        else {
            LOG.trace("Keycloak is not enabled");
        }

        //sort the batch splits by sequence ID
        unnotifiedBatchSplits = unnotifiedBatchSplits
                .stream()
                .sorted(Comparator.comparing(t -> t.getBatch().getSequenceNumber()))
                .collect(Collectors.toList());

        //hash the split batches by organisation ID and keep an ordered list of the organisations,
        //so we notify the earliest received organisations first
        HashMap<String, List<BatchSplit>> hmByOrg = new HashMap<>();
        List<String> organisationIds = new ArrayList<>();

        for (BatchSplit batchSplit: unnotifiedBatchSplits) {
            List<BatchSplit> list = hmByOrg.get(batchSplit.getOrganisationId());

            if (list == null) {
                list = new ArrayList<>();
                hmByOrg.put(batchSplit.getOrganisationId(), list);
                organisationIds.add(batchSplit.getOrganisationId());
            }

            list.add(batchSplit);
        }

        //then attempt to notify EDS for each organisation
        int countSuccess = 0;
        int countFail = 0;

        for (String organisationId: organisationIds) {
            List<BatchSplit> batchSplits = hmByOrg.get(organisationId);

            try {
                for (BatchSplit batchSplit: batchSplits) {

                    LOG.trace("Notifying EDS for batch split: {}", batchSplit.getBatchSplitId());
                    notify(batchSplit);
                    countSuccess ++;
                }
            } catch (Exception e) {
                countFail ++;
                LOG.error("Error occurred notifying EDS for batch split", e);
            }
        }

        LOG.info("Notified EDS successfully {} times and failed {}", countSuccess, countFail);
    }


    private void notify(BatchSplit unnotifiedBatchSplit) throws SftpReaderException, PgStoredProcException, IOException {

        UUID messageId = UUID.randomUUID();
        int batchSplitId = unnotifiedBatchSplit.getBatchSplitId();
        String organisationId = unnotifiedBatchSplit.getOrganisationId();
        String softwareContentType = dbConfiguration.getSoftwareContentType();
        String softwareVersion = dbConfiguration.getSoftwareVersion();
        /*String softwareContentType = dbInstanceConfiguration.getEdsConfiguration().getSoftwareContentType();
        String softwareVersion = dbInstanceConfiguration.getEdsConfiguration().getSoftwareVersion();*/

        String outboundMessage = null;

        try {
            SftpNotificationCreator sftpNotificationCreator = ImplementationActivator.createSftpNotificationCreator(dbConfiguration.getInterfaceTypeName());
            String messagePayload = sftpNotificationCreator.createNotificationMessage(organisationId, db, dbConfiguration, unnotifiedBatchSplit);

            EdsSenderResponse edsSenderResponse = null;

            if (Strings.isNullOrEmpty(messagePayload)) {
                //if an empty message payload is returned, this means to NOT fail the batch but also not notify the messaging API for it
                //which allows us to skip earlier extracts for an organisation received BEFORE a bulk or re-bulk
                edsSenderResponse = new EdsSenderResponse();
                edsSenderResponse.setStatusLine("Not Sent To Messaging API");
                edsSenderResponse.setResponseBody("Outbound message payload was empty, so message not being sent to Messaging API");

            } else {
                outboundMessage = EdsSender.buildEnvelope(messageId, organisationId, softwareContentType, softwareVersion, messagePayload);

                String edsUrl = dbInstanceConfiguration.getEdsConfiguration().getEdsUrl();
                boolean useKeycloak = dbInstanceConfiguration.getEdsConfiguration().isUseKeycloak();

                edsSenderResponse = EdsSender.notifyEds(edsUrl, useKeycloak, outboundMessage);
            }

            db.addBatchNotification(unnotifiedBatchSplit.getBatchId(),
                    batchSplitId,
                    dbConfiguration.getConfigurationId(),
                    messageId,
                    outboundMessage,
                    edsSenderResponse.getStatusLine() + "\r\n" + edsSenderResponse.getResponseBody(),
                    true,
                    null);

            //notify to Slack to say any previous error is now cleared, so we don't have to keep monitoring files
            if (shouldSendSlackOk(batchSplitId)) {
                sendSlackOk(batchSplitId, organisationId);
            }

        } catch (Exception e) {
            String inboundMessage = null;

            if (e instanceof EdsSenderHttpErrorResponseException) {
                EdsSenderResponse edsSenderResponse = ((EdsSenderHttpErrorResponseException)e).getEdsSenderResponse();
                inboundMessage = edsSenderResponse.getStatusLine() + "\r\n" + edsSenderResponse.getResponseBody();
            }

            db.addBatchNotification(unnotifiedBatchSplit.getBatchId(),
                    batchSplitId,
                    dbConfiguration.getConfigurationId(),
                    messageId,
                    outboundMessage,
                    inboundMessage,
                    false,
                    getExceptionNameAndMessage(e));

            //notify to Slack, so we don't have to keep monitoring files
            if (shouldSendSlackAlert(batchSplitId, inboundMessage)) {
                sendSlackAlert(batchSplitId, organisationId, inboundMessage);
            }

            throw new SftpReaderException("Error notifying EDS for batch split " + batchSplitId, e);
        }
    }


    private static String getExceptionNameAndMessage(Throwable e) {
        String result = "[" + e.getClass().getName() + "] " + e.getMessage();

        if (e.getCause() != null)
            result += " | " + getExceptionNameAndMessage(e.getCause());

        return result;
    }

    private LocalDateTime calculateNextRunTime(LocalDateTime thisRunStartTime) {
        Validate.notNull(thisRunStartTime);

        return thisRunStartTime.plusSeconds(dbConfiguration.getPollFrequencySeconds());
    }


    private void sendSlackAlert(int batchSplitId, String organisationId, String errorMessage) {

        SftpOrganisationHelper orgHelper = ImplementationActivator.createSftpOrganisationHelper(dbConfiguration.getInterfaceTypeName());
        String organisationName = orgHelper.findOrganisationNameFromOdsCode(db, organisationId);
        String message = "Exception notifying batch for Organisation " + organisationId + ", " + organisationName + " and Batch Spit " + batchSplitId + "\r\n" + errorMessage;

        SlackNotifier slackNotifier = new SlackNotifier(configuration);
        slackNotifier.postMessage(message);

        //add to the map so we don't send the same message again in a few minutes
        notificationErrorrs.put(batchSplitId, errorMessage);
    }

    private boolean shouldSendSlackAlert(int batchSplitId, String errorMessage) {

        if (!notificationErrorrs.containsKey(new Integer(batchSplitId))) {
            return true;
        }

        //don't keep sending the alert for the same error message
        String previousError = notificationErrorrs.get(new Integer(batchSplitId));
        if (previousError == null && errorMessage == null) {
            return false;
        }

        if (previousError != null
                && errorMessage != null
                && previousError.equals(errorMessage)) {
            return false;
        }

        return true;
    }

    private boolean shouldSendSlackOk(int batchSplitId) {
        return notificationErrorrs.containsKey(new Integer(batchSplitId));
    }

    private void sendSlackOk(int batchSplitId, String organisationId) {

        SftpOrganisationHelper orgHelper = ImplementationActivator.createSftpOrganisationHelper(dbConfiguration.getInterfaceTypeName());
        String organisationName = orgHelper.findOrganisationNameFromOdsCode(db, organisationId);
        String message = "Previous error notifying Messaging API for Organisation " + organisationId + ", " + organisationName + " and Batch Split " + batchSplitId + " is now cleared";

        SlackNotifier slackNotifier = new SlackNotifier(configuration);
        slackNotifier.postMessage(message);

        //remove from the map, so we know we're in a good state now
        notificationErrorrs.remove(batchSplitId);
    }
}
