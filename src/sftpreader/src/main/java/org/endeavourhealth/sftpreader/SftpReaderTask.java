package org.endeavourhealth.sftpreader;

import com.google.common.base.Strings;
import com.jcraft.jsch.JSchException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.Header;
import org.endeavourhealth.common.eds.EdsSender;
import org.endeavourhealth.common.eds.EdsSenderHttpErrorResponseException;
import org.endeavourhealth.common.eds.EdsSenderResponse;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.implementations.*;
import org.endeavourhealth.sftpreader.model.ConfigurationLockI;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.sources.Connection;
import org.endeavourhealth.sftpreader.sources.ConnectionActivator;
import org.endeavourhealth.sftpreader.sources.ConnectionDetails;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

//import org.endeavourhealth.common.config.ConfigManager;

public class SftpReaderTask implements Runnable {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpReaderTask.class);

    private static Map<Integer, String> notificationErrorrs = new HashMap<>();

    private static final String SHOULD_PAUSE_NOTIFYING = "PauseNotify";


    private Configuration configuration = null;
    private String configurationId = null;
    private DbInstance dbInstanceConfiguration = null;
    private DbConfiguration dbConfiguration = null;
    private DataLayerI db = null;

    public SftpReaderTask(Configuration configuration, String configurationId) {
        this.configuration = configuration;
        this.configurationId = configurationId;
    }

    @Override
    public void run() {

        ConfigurationLockI lock = null;
        try {
            LOG.trace(">>>Starting scheduled SftpReader run, initialising");
            initialise();

            //create a DB-level advisory lock, so no other application can process this configuration at the same time
            LOG.trace(">>>Getting exclusive lock for configuration " + this.configurationId);
            lock = db.createConfigurationLock("SftpReader-" + configurationId);

            LOG.trace(">>>Downloading and decrypting files");
            downloadNewFiles();

            LOG.trace(">>>Checking for unknown files");
            validateUnknownFiles();

            LOG.trace(">>>Sequencing batches");
            List<Batch> incompleteBatches = sequenceBatches();

            Batch lastCompleteBatch = db.getLastCompleteBatch(dbConfiguration.getConfigurationId());

            for (Batch incompleteBatch: incompleteBatches) {

                LOG.trace(">>>Unzipping/decrypting batch " + incompleteBatch.getBatchId());
                unzipDecryptBatch(incompleteBatch);

                LOG.trace(">>>Validating batch " + incompleteBatch.getBatchId());
                if (!validateBatch(incompleteBatch, lastCompleteBatch)) {
                    //if the batch fails validation without throwing an exception, just break out leave it as uncomplete
                    break;
                }

                LOG.trace(">>>Splitting batch " + incompleteBatch.getBatchId());
                splitBatch(incompleteBatch, lastCompleteBatch);

                LOG.trace(">>>Post-Split Validating batch " + incompleteBatch.getBatchId());
                postSplitValidateBatch(incompleteBatch, lastCompleteBatch);

                LOG.trace(">>>Complete batch " + incompleteBatch.getBatchId());
                completeBatch(incompleteBatch);

                LOG.trace(">>>Deleting temp file for batch " + incompleteBatch.getBatchId());
                deleteTempFiles(incompleteBatch);

                lastCompleteBatch = incompleteBatch;
            }

            LOG.trace(">>>Notifying EDS");
            notifyEds();

            LOG.trace(">>>Completed SftpReader run");

        } catch (Exception e) {
            LOG.error(">>>Fatal exception in SftpTask run, terminating this run", e);

            //send slack alert, so we don't miss it
            SlackNotifier.postMessage("Exception in SFTP Reader for " + this.configurationId, e);

        } finally {
            //delete any previous temp files that were left around
            deleteTempDir();

            //release the configuration lock
            if (lock != null) {
                try {
                    lock.releaseLock();
                } catch (Exception ex) {
                    LOG.error("", ex);
                }
            }
        }
    }

    private void deleteTempDir() {
        DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();
        String tempRootDir = edsConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String tempDir = FilenameUtils.concat(tempRootDir, configurationDir);

        try {
            FileHelper.deleteRecursiveIfExists(tempDir);
        } catch (Exception ex) {
            LOG.error("Error deleting " + tempDir, ex);
        }
    }

    private void deleteTempFiles(Batch batch) throws Exception {

        String tempStoragePath = dbInstanceConfiguration.getEdsConfiguration().getTempDirectory();

        String configurationPath = dbConfiguration.getLocalRootPath();
        String batchPath = batch.getLocalRelativePath();

        String tempDir = FilenameUtils.concat(tempStoragePath, configurationPath);
        tempDir = FilenameUtils.concat(tempDir, batchPath);

        File f = new File(tempDir);

        //if we've not actually had to do anything with a batch's files, there won't be a temp dir for it
        if (f.exists()) {
            FileUtils.forceDelete(f);
        }
    }

    private void completeBatch(Batch batch) throws Exception {

        //mark as complete on the DB
        db.setBatchAsComplete(batch);

        //and tell Slack
        SlackNotifier.notifyCompleteBatch(dbConfiguration, batch);
    }

    private void unzipDecryptBatch(Batch batch) throws Exception {

        SftpBatchUnzipperDecrypter unzipper = ImplementationActivator.createSftpUnzipperDecrypter(dbConfiguration);
        unzipper.unzipAndDecrypt(batch, dbInstanceConfiguration.getEdsConfiguration(), dbConfiguration, db);
    }

    private void initialise() throws Exception {
        this.dbInstanceConfiguration = configuration.getInstanceConfiguration();
        this.dbConfiguration = configuration.getConfiguration(configurationId);
        this.db = configuration.getDataLayer();
        //checkLocalRootPathPrefixExists();

        //the code used to support having the same temp and storage dirs, but this caused more problems
        //than it solved. So just validate that they're NOT the same
        DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();
        String sharedStorageDir = edsConfiguration.getSharedStoragePath();
        String tempDir = edsConfiguration.getTempDirectory();

        if (FilenameUtils.equals(tempDir, sharedStorageDir)) {
            throw new Exception("Temp and Storage dirs are the same - temp dir must be different");
        }
    }

    /*private void checkLocalRootPathPrefixExists() throws Exception {
        if (StringUtils.isNotEmpty(this.dbConfiguration.getLocalRootPathPrefix())) {

            File rootPath = new File(this.dbConfiguration.getLocalRootPathPrefix());

            if ((!rootPath.exists()) || (!rootPath.isDirectory()))
                throw new SftpReaderException("Local root path prefix '" + rootPath + "' does not exist");
        }
    }*/

    private void downloadNewFiles() throws SftpReaderException {
        Connection connection = null;

        try {
            connection = openSftpConnection(dbConfiguration.getSftpConfiguration());

            String remotePath = dbConfiguration.getSftpConfiguration().getRemotePath();

            List<RemoteFile> remoteFiles = getFileList(connection, remotePath);

            int countAlreadyProcessed = 0;

            LOG.trace("Found " + remoteFiles.size() + " files in " + remotePath);

            for (RemoteFile remoteFile : remoteFiles) {
                SftpFile batchFile = instantiateSftpBatchFile(remoteFile);

                if (!batchFile.isFileNeeded()) {
                    //LOG.trace("   Filename not needed, skipping: " + batchFile.getFilename());
                    continue;
                }

                if (!batchFile.isFilenameValid()) {
                    LOG.error("   Invalid filename, skipping: " + batchFile.getFilename());
                    if (!batchFile.ignoreUnknownFileTypes()) {

                        boolean newUnknownFile = db.addUnknownFile(dbConfiguration.getConfigurationId(), batchFile);

                        //if it's a new unknown file, send a Slack message about it
                        if (newUnknownFile) {
                            String message = "New unknown file for " + dbConfiguration.getSoftwareContentType() + ": " + batchFile.getFilename();
                            SlackNotifier.postMessage(message);
                        }
                    }
                    continue;
                }

                AddFileResult addFileResult = db.addFile(configurationId, batchFile);

                if (addFileResult.isFileAlreadyProcessed()) {
                    countAlreadyProcessed ++;
                    continue;
                }

                batchFile.setBatchFileId(addFileResult.getBatchFileId());

                downloadFile(connection, batchFile);
            }

            if (countAlreadyProcessed > 0) {
                LOG.trace("Skipped " + countAlreadyProcessed + " files as already downloaded them");
            }

            LOG.info("Completed processing {} files", Integer.toString(remoteFiles.size()));

        } catch (Exception e) {
            throw new SftpReaderException("Exception occurred while downloading files - cannot continue or may process batches out of order", e);

        } finally {
            closeConnection(connection);
        }
    }

    private Connection openSftpConnection(DbConfigurationSftp configurationSftp) throws Exception, JSchException, IOException {
        ConnectionDetails connectionDetails = getSftpConnectionDetails(configurationSftp);

        String hostname = connectionDetails.getHostname();
        String port = Integer.toString(connectionDetails.getPort());
        String username = connectionDetails.getUsername();
        Connection connection = ConnectionActivator.createConnection(connectionDetails);

        LOG.info("Opening " + connection.getClass().getName() + " to " + hostname + " on port " + port + " with user " + username);

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

    /**
     * temporarily changed to download directly as a .GPG file rather than a .download file
     */
    private void downloadFile(Connection connection, SftpFile sftpFile) throws Exception {

        String fileName = sftpFile.getFilename();
        String tempRootDir = dbInstanceConfiguration.getEdsConfiguration().getTempDirectory();
        String localRootDir = sftpFile.getLocalPath();
        String tempDir = FilenameUtils.concat(tempRootDir, localRootDir);

        File downloadDestination = new File(tempDir, fileName);
        LOG.info("Downloading file to: " + downloadDestination);

        //ensure the download directory exists
        File downloadDestinationDir = downloadDestination.getParentFile();
        if (!downloadDestinationDir.exists()) {
            if (!downloadDestinationDir.mkdirs()) {
                throw new Exception("Failed to create directory" + downloadDestinationDir);
            }
        }

        //delete any previously downloaded file
        if (downloadDestination.exists()) {
            if (!downloadDestination.delete()) {
                throw new IOException("Could not delete existing temporary download file " + downloadDestination);
            }
        }

        //download the file to our temp directory
        String remoteFilePath = sftpFile.getRemoteFilePath();
        InputStream inputStream = connection.getFile(remoteFilePath);
        Files.copy(inputStream, downloadDestination.toPath());

        long fileLen = downloadDestination.length();

        //move the file to our permanent storage
        String sharedStoragePath = dbInstanceConfiguration.getEdsConfiguration().getSharedStoragePath();
        String storageDestinationPath = FilenameUtils.concat(sharedStoragePath, localRootDir);
        storageDestinationPath = FilenameUtils.concat(storageDestinationPath, fileName);

        LOG.info("Writing to permanent storage: " + storageDestinationPath);
        FileHelper.writeFileToSharedStorage(storageDestinationPath, downloadDestination);

        //and delete from our temporary storage
        downloadDestination.delete();

        //update the DB to confirm we've downloaded it
        sftpFile.setLocalFileSizeBytes(fileLen);
        db.setFileAsDownloaded(sftpFile);
    }
    /*private void downloadFile(Connection connection, SftpFile batchFile) throws Exception {
        String localFilePath = batchFile.getLocalFilePath();
        LOG.info("Downloading file to: " + localFilePath);

        File destination = new File(localFilePath);

        if (destination.exists()) {
            if (!destination.delete()) {
                throw new IOException("Could not delete existing download file " + localFilePath);
            }
        }

        String remoteFilePath = batchFile.getRemoteFilePath();

        InputStream inputStream = connection.getFile(remoteFilePath);
        Files.copy(inputStream, destination.toPath());

        batchFile.setLocalFileSizeBytes(getFileSizeBytes(batchFile.getLocalFilePath()));

        db.setFileAsDownloaded(batchFile);
    }*/

    private SftpFile instantiateSftpBatchFile(RemoteFile remoteFile) throws Exception {

        SftpFilenameParser sftpFilenameParser = ImplementationActivator.createFilenameParser(true, remoteFile, dbConfiguration);
        String configurationStorageDir = dbConfiguration.getLocalRootPath();

        return new SftpFile(remoteFile,
                sftpFilenameParser,
                configurationStorageDir);
    }

    /*private void createBatchDirectory(SftpFile batchFile) throws IOException {
        File localPath = new File(batchFile.getLocalPath());

        if (!localPath.exists())
            if (!localPath.mkdirs())
                throw new IOException("Could not create path " + localPath);
    }*/

    /*private void deleteRemoteFile(SftpConnection sftpConnection, String remoteFilePath) throws SftpException {
        LOG.info("Deleting remote file " + remoteFilePath);

        sftpConnection.deleteFile(remoteFilePath);
    }*/


    /*private void decryptFile(SftpFile batchFile) throws Exception {
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
    }*/

    /*private static long getFileSizeBytes(String filePath) {
        File file = new File(filePath);
        return file.length();
    }*/

    private List<UnknownFile> getUnknownFiles() throws Exception {
        return db.getUnknownFiles(dbConfiguration.getConfigurationId());
    }

    private List<Batch> getIncompleteBatches() throws Exception {
        LOG.trace(" Getting batches ready for validation and sequencing");

        List<Batch> incompleteBatches = db.getIncompleteBatches(dbConfiguration.getConfigurationId());

        LOG.trace(" There are {} batches ready for validation and sequencing", Integer.toString(incompleteBatches.size()));

        return incompleteBatches;
    }

    private void validateUnknownFiles() throws Exception {

        //if we downloaded any files that don't fit with our expectation, they'll be logged as an unknown file
        List<UnknownFile> unknownFiles = getUnknownFiles();
        if (unknownFiles.size() > 0) {
            throw new SftpValidationException("There are " + Integer.toString(unknownFiles.size()) + " unknown files present.");
        }
    }

    private boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch) throws Exception {

        LOG.trace(" Validating batches " + incompleteBatch.getBatchIdentifier());

        SftpBatchValidator sftpBatchValidator = ImplementationActivator.createSftpBatchValidator(dbConfiguration);
        boolean valid = sftpBatchValidator.validateBatch(incompleteBatch, lastCompleteBatch, dbInstanceConfiguration.getEdsConfiguration(), dbConfiguration, db);

        LOG.trace(" Completed batch validation");
        return valid;
    }

    private void postSplitValidateBatch(Batch incompleteBatch, Batch lastCompleteBatch) throws Exception {

        LOG.trace(" Post-Split Validating batches " + incompleteBatch.getBatchIdentifier());

        SftpPostSplitBatchValidator sftpBatchValidator = ImplementationActivator.createSftpPostSplitBatchValidator(dbConfiguration);
        sftpBatchValidator.validateBatchPostSplit(incompleteBatch, lastCompleteBatch, dbInstanceConfiguration.getEdsConfiguration(), dbConfiguration, db);
    }

    private List<Batch> sequenceBatches() throws Exception {
        LOG.trace(" Sequencing batches");

        List<Batch> incompleteBatches = getIncompleteBatches();

        if (incompleteBatches.isEmpty()) {
            return new ArrayList<>();
        }

        Batch lastCompleteBatch = db.getLastCompleteBatch(dbConfiguration.getConfigurationId());
        int nextSequenceNumber = getNextSequenceNumber(lastCompleteBatch);

        SftpBatchSequencer sftpBatchSequencer = ImplementationActivator.createSftpBatchSequencer(dbConfiguration);
        Map<Batch, Integer> batchSequence = sftpBatchSequencer.determineBatchSequenceNumbers(incompleteBatches, nextSequenceNumber, lastCompleteBatch);

        Map<Batch, Integer> sortedBatchSequence = StreamExtension.sortByValue(batchSequence);

        if (!new HashSet<>(incompleteBatches).equals(sortedBatchSequence.keySet())) {
            throw new SftpValidationException("Batch sequence does not contain all unsequenced batches");
        }

        for (Batch batch : sortedBatchSequence.keySet()) {
            if (sortedBatchSequence.get(batch).intValue() != nextSequenceNumber++) {
                throw new SftpValidationException("Unexpected batch sequence number");
            }
        }

        for (Batch batch : sortedBatchSequence.keySet()) {
            LOG.debug("  Batch " + batch.getBatchIdentifier() + " sequenced as " + sortedBatchSequence.get(batch).toString());

            db.setBatchSequenceNumber(batch, sortedBatchSequence.get(batch));
        }

        LOG.trace(" Completed batch sequencing");

        return new ArrayList<>(sortedBatchSequence.keySet());
    }

    private void splitBatch(Batch batch, Batch lastCompleteBatch) throws Exception {

        //delete any pre-existing splits for this batch
        db.deleteBatchSplits(batch);

        SftpBatchSplitter sftpBatchSplitter = ImplementationActivator.createSftpBatchSplitter(dbConfiguration);

        List<BatchSplit> splitBatches = sftpBatchSplitter.splitBatch(batch, lastCompleteBatch, db, dbInstanceConfiguration.getEdsConfiguration(), dbConfiguration);

        for (BatchSplit splitBatch: splitBatches) {
            splitBatch.setConfigurationId(dbConfiguration.getConfigurationId());
            db.addBatchSplit(splitBatch);
        }
    }

    private static int getNextSequenceNumber(Batch lastCompleteBatch) {
        if (lastCompleteBatch == null)
            return 1;

        return lastCompleteBatch.getSequenceNumber() + 1;
    }

    private void notifyEds() throws Exception {

        //TODO - add support to pause notifying of EDS, so we still collect new files but hold off on posting them to the Messaging API
        boolean shouldPauseNotifying = false;
        for (DbConfigurationKvp dbConfigurationKvp : dbConfiguration.getDbConfigurationKvp()) {
            if (dbConfigurationKvp.getKey().equals(SHOULD_PAUSE_NOTIFYING)) {
                shouldPauseNotifying = Boolean.parseBoolean(dbConfigurationKvp.getValue());
                break;
            }
        }
        if (shouldPauseNotifying) {
            LOG.info("Skipping notifying Messaging API as paused");
            return;
        }

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
            String orgId = batchSplit.getOrganisationId();
            List<BatchSplit> list = hmByOrg.get(orgId);

            if (list == null) {
                list = new ArrayList<>();
                hmByOrg.put(batchSplit.getOrganisationId(), list);
                organisationIds.add(orgId);
            }

            list.add(batchSplit);
        }

        //then attempt to notify EDS for each organisation
        int countSuccess = 0;
        int countFail = 0;

        for (String organisationId: organisationIds) {
            List<BatchSplit> batchSplits = hmByOrg.get(organisationId);

            try {
                //perform any pre-notification checking/fixing on the data required


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


    private void notify(BatchSplit unnotifiedBatchSplit) throws Exception {

        UUID messageId = UUID.randomUUID();
        int batchSplitId = unnotifiedBatchSplit.getBatchSplitId();
        String organisationId = unnotifiedBatchSplit.getOrganisationId();
        String softwareContentType = dbConfiguration.getSoftwareContentType();
        String softwareVersion = dbConfiguration.getSoftwareVersion();
        /*String softwareContentType = dbInstanceConfiguration.getEdsConfiguration().getSoftwareContentType();
        String softwareVersion = dbInstanceConfiguration.getEdsConfiguration().getSoftwareVersion();*/

        String outboundMessage = null;

        try {
            SftpNotificationCreator sftpNotificationCreator = ImplementationActivator.createSftpNotificationCreator(dbConfiguration);
            String messagePayload = sftpNotificationCreator.createNotificationMessage(organisationId, db, dbInstanceConfiguration.getEdsConfiguration(), dbConfiguration, unnotifiedBatchSplit);

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


    private void sendSlackAlert(int batchSplitId, String organisationId, String errorMessage) throws Exception {

        String publisherSoftware = dbConfiguration.getSoftwareContentType(); //e.g. EMISCSV, VISIONCSV
        SftpOrganisationHelper orgHelper = ImplementationActivator.createSftpOrganisationHelper(dbConfiguration);
        String organisationName = orgHelper.findOrganisationNameFromOdsCode(db, organisationId);

        //LOG.info("Going to send Slack alert about batch " + batchSplitId + " and org ID [" + organisationId + "] and software [" + publisherSoftware + "]: " + errorMessage);
        //LOG.info("Got org name " + organisationName + " from " + orgHelper.getClass().getName());

        String message = "Exception notifying " + publisherSoftware + " batch for Organisation " + organisationId + ", " + organisationName + " and Batch Split " + batchSplitId + "\r\n" + errorMessage;

        SlackNotifier.postMessage(message);

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

    private void sendSlackOk(int batchSplitId, String organisationId) throws Exception {

        SftpOrganisationHelper orgHelper = ImplementationActivator.createSftpOrganisationHelper(dbConfiguration);
        String organisationName = orgHelper.findOrganisationNameFromOdsCode(db, organisationId);
        String message = "Previous error notifying Messaging API for Organisation " + organisationId + ", " + organisationName + " and Batch Split " + batchSplitId + " is now cleared";

        SlackNotifier.postMessage(message);

        //remove from the map, so we know we're in a good state now
        notificationErrorrs.remove(batchSplitId);
    }
}
