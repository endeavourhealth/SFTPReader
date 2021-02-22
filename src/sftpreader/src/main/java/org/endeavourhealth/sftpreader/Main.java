package org.endeavourhealth.sftpreader;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.http.Header;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.common.utility.MetricsHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.core.application.ApplicationHeartbeatHelper;
import org.endeavourhealth.core.database.dal.usermanager.caching.OrganisationCache;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.AdastraBulkDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.AdastraDateDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraConstants;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraHelper;
import org.endeavourhealth.sftpreader.implementations.barts.BartsBulkDetector;
import org.endeavourhealth.sftpreader.implementations.barts.BartsDateDetector;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsConstants;
import org.endeavourhealth.sftpreader.implementations.bhrut.BhrutBulkDetector;
import org.endeavourhealth.sftpreader.implementations.bhrut.BhrutDateDetector;
import org.endeavourhealth.sftpreader.implementations.bhrut.BhrutFilenameParser;
import org.endeavourhealth.sftpreader.implementations.emis.EmisBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisFixDisabledService;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.implementations.emisCustom.EmisCustomBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emisCustom.utility.EmisCustomConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.TppBulkDetector;
import org.endeavourhealth.sftpreader.implementations.tpp.TppDateDetector;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppHelper;
import org.endeavourhealth.sftpreader.implementations.vision.VisionBulkDetector;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionConstants;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException;
import org.endeavourhealth.sftpreader.sender.DpaCheck;
import org.endeavourhealth.sftpreader.utilities.PgpUtil;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.Proxy;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;


public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static SftpReaderTaskScheduler sftpReaderTaskScheduler;
    //private static ManagementService managementService;
    //private static SlackNotifier slackNotifier;


	public static void main(String[] args) {
		try {
            String instanceName = null;
            if (args.length > 0) {
                instanceName = args[0];
            } else {
                //instance name should be passed as application argument for consistency with other apps,
                //but support getting it via VM property for legacy support
                instanceName = System.getProperty("INSTANCE_NAME");
            }
            ConfigManager.initialize("sftpreader", instanceName);

            configuration = Configuration.getInstance();

            /*if (args.length > 0) {
                if (args[0].equals("TestS3")) {
                    testS3(args);
                    System.exit(0);
                }
            }*/

            /*if (args.length > 0) {
                if (args[0].equalsIgnoreCase("TestSplittingAndJoining")) {
                    testSplittingAndJoining();
                    System.exit(0);
                }
            }*/



            if (args.length > 1) {
                /*if (args[0].equalsIgnoreCase("FixS3")) {
                    String bucket = args[1];
                    String path = args[2];
                    boolean test = Boolean.parseBoolean(args[3]);
                    fixS3(bucket, path, test);
                    System.exit(0);
                }*/

                /*if (args[0].equalsIgnoreCase("TestLargeCopy")) {
                    String bucket = args[1];
                    String srcKey = args[2];
                    String dstKey = args[3];
                    long maxChunk = Long.parseLong(args[4]);
                    testCopyLargeFile(bucket, srcKey, dstKey, maxChunk);
                    System.exit(0);
                }*/

                /*if (args[0].equalsIgnoreCase("Test7z")) {
                    String file = args[1];
                    String password = args[2];
                    test7zDecompress(file, password);
                    System.exit(0);
                }*/
                if (args[1].equalsIgnoreCase("DecryptGpg")) {
                    String configurationId = args[2];
                    String srcFilePath = args[3];
                    String dstFilePath = args[4];

                    decryptGpgFile(configurationId, srcFilePath, dstFilePath);
                    System.exit(0);
                }

                /*if (args[1].equalsIgnoreCase("PopulateExtractDates")) {
                    boolean testMode = Boolean.parseBoolean(args[2]);
                    String configurationId = args[3];
                    populateExtractDates(testMode, configurationId);
                    System.exit(0);
                }*/

                if (args[1].equalsIgnoreCase("PopulateHasPatientData")) {
                    boolean testMode = Boolean.parseBoolean(args[2]);
                    String configurationId = args[3];
                    String odsRegex = null;
                    if (args.length > 4) {
                        odsRegex = args[4];
                    }
                    populateHasPatientData(testMode, configurationId, odsRegex);
                    System.exit(0);
                }

                if (args[1].equalsIgnoreCase("TestDPA")) {
                    String odsCode = args[2];
                    testDpa(odsCode);
                    System.exit(0);
                }

                /*if (args[0].equalsIgnoreCase("TestOriginalTerms")) {
                    testOriginalTerms();
                    System.exit(0);
                }*/

                /*if (args[0].equalsIgnoreCase("TestDisabledFix")) {
                    String odsCode = args[1];
                    String configurationId = args[2];
                    testDisabledFix(odsCode, configurationId);
                    System.exit(0);
                }*/

                /*if (args[0].equalsIgnoreCase("TestS3Tags")) {
                    String s3Path = args[1];
                    String tag = args[2];
                    String tagValue = null;
                    if (args.length > 3) {
                        tagValue = args[3];
                    }

                    testS3Tags(s3Path, tag, tagValue);
                    System.exit(0);
                }

                if (args[0].equalsIgnoreCase("FixEmisS3Tags")) {
                    String configurationId = args[1];
                    fixEmisS3Tags(configurationId);
                    System.exit(0);
                }*/

                if (args[0].equalsIgnoreCase("CheckS3")) {
                    String path = args[1];
                    checkS3(path);
                    System.exit(0);
                }

                if (args[0].equalsIgnoreCase("ReadS3Bytes")) {
                    String path = args[1];
                    long start = Long.parseLong(args[2]);
                    long len = Long.parseLong(args[3]);
                    readS3Bytes(path, start, len);
                    System.exit(0);
                }

                /*if (args[0].equalsIgnoreCase("TestSplitting")) {
                    String path = args[1];
                    testSplitting(path);
                    System.exit(0);
                }*/

                if (args.length > 1
                    && args[1].equalsIgnoreCase("CheckForBulks")) {
                    String configurationId = args[2];
                    boolean testMode = Boolean.parseBoolean(args[3]);
                    String odsRegex = null;
                    if (args.length > 4) {
                        odsRegex = args[4];
                    }
                    checkForBulks(configurationId, testMode, odsRegex);
                    System.exit(0);
                }

                /*if (args.length > 1
                        && args[1].equalsIgnoreCase("CopyTppManifestFiles")) {
                    String configurationId = args[2];
                    boolean testMode = Boolean.parseBoolean(args[3]);
                    copyTppManifestFiles(configurationId, testMode);
                    System.exit(0);
                }*/

                /*if (args.length > 1
                        && args[1].equalsIgnoreCase("TestVisionSlackAlert")) {
                    testVisionSlackAlert();
                    System.exit(0);
                }*/

                /*if (args.length > 1
                        && args[1].equalsIgnoreCase("FixTppOutOfOrderData")) {
                    String configurationId = args[2];
                    boolean testMode = Boolean.parseBoolean(args[3]);
                    String odsRegex = null;
                    if (args.length > 4) {
                        odsRegex = args[4];
                    }
                    fixTppOutOfOrderData(configurationId, testMode, odsRegex);
                    System.exit(0);
                }*/

                //One-off data load for TPP
                if (args.length > 1
                        && args[1].equalsIgnoreCase("loadTppSRCode")) {
                    String configurationId = args[2];
                    String odsCodeRegex = null;
                    if (args.length > 3) {
                        odsCodeRegex = args[3];
                    }
                    SRCodeLoader.loadTppSRCodeIntoHashTable(configuration, configurationId, odsCodeRegex);
                    System.exit(0);
                }

                //testing SQL connection to DSM
                if (args.length > 1
                        && args[1].equalsIgnoreCase("checkDpa")) {
                    String odsCode = args[2];
                    Boolean hasDpa = OrganisationCache.doesOrganisationHaveDPA(odsCode);
                    LOG.debug("DPA check for " + odsCode + " = " + hasDpa);
                    System.exit(0);
                }

                /*if (args[1].equalsIgnoreCase("RecreateBatchFile")) {
                    String configurationId = args[2];
                    recreateBatchFile(configurationId);
                    System.exit(0);
                }*/

                /*if (args.length > 1
                    && args[1].equalsIgnoreCase("RecreateBatchFile2")) {
                    String configurationId = args[2];
                    recreateBatchFile2(configurationId);
                    System.exit(0);
                }*/

                if (args.length > 1
                        && args[1].equalsIgnoreCase("testSlack")) {
                    testSlack();
                    System.exit(0);
                }
            }

            /*if (args.length > 0) {
                if (args[0].equalsIgnoreCase("DeleteCsvs")) {
                    String bucket = args[1];
                    String path = args[2];
                    boolean test = Boolean.parseBoolean(args[3]);
                    deleteCsvs(bucket, path, test);
                    System.exit(0);
                }
            }*/

            LOG.info("--------------------------------------------------");
            LOG.info("SFTP Reader " + instanceName);
            LOG.info("--------------------------------------------------");

            LOG.info("Instance " + configuration.getInstanceName() + " on host " + configuration.getMachineName());
            LOG.info("Processing configuration(s): " + configuration.getConfigurationIdsForDisplay());

            /*slackNotifier = new SlackNotifier(configuration);
            slackNotifier.notifyStartup();*/

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

            sftpReaderTaskScheduler = new SftpReaderTaskScheduler(configuration);

            //start both these off, so we know this app is running
            MetricsHelper.startHeartbeat();
            ApplicationHeartbeatHelper.start(sftpReaderTaskScheduler); //this cannot work if SFTP Reader cannot access core DB server

            //and start working
            sftpReaderTaskScheduler.start();

        } catch (ConfigManagerException cme) {
            printToErrorConsole("Fatal exception occurred initializing ConfigManager", cme);
            LOG.error("Fatal exception occurred initializing ConfigManager", cme);
            System.exit(-2);
        }
        catch (Exception e) {
            LOG.error("Fatal exception occurred", e);
            System.exit(-1);
        }
	}

    private static void testSlack() {
		LOG.info("Testing slack");

		try {
            LOG.debug("Testing receipts channel");
			SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderReceipts, "Test Message from SFTP Reader for Receipts channel");

            LOG.debug("Testing alerts channel");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, "Test Message from SFTP Reader for Alerts channel");

            LOG.debug("Testing ODS search");
            Proxy proxy = SlackHelper.getProxy();
            OdsOrganisation r = OdsWebService.lookupOrganisationViaRest("F84062", proxy);
            LOG.debug("Got result " + (r == null? "null" : r.getOrganisationName()));

			LOG.info("Finished testing slack");

		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

    private static void testDpa(String odsCode) {
        LOG.debug("Testing DPA for " + odsCode);
        try {

            DbInstance dbInstanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();

            if (edsConfiguration.isUseKeycloak()) {
                LOG.trace("Initialising keycloak at: {}", edsConfiguration.getKeycloakTokenUri());

                try {
                    KeycloakClient.init(edsConfiguration.getKeycloakTokenUri(),
                            edsConfiguration.getKeycloakRealm(),
                            edsConfiguration.getKeycloakUsername(),
                            edsConfiguration.getKeycloakPassword(),
                            edsConfiguration.getKeycloakClientId());

                    Header response = KeycloakClient.instance().getAuthorizationHeader();
                    LOG.trace("Keycloak authorization header is {}: {}", response.getName(), response.getValue());

                } catch (IOException e) {
                    throw new SftpReaderException("Error initialising keycloak", e);
                }

            } else {
                LOG.trace("Keycloak is not enabled");
            }

            String edsUrl = dbInstanceConfiguration.getEdsConfiguration().getEdsUrl();

            boolean hasDpa = DpaCheck.checkForDpa(odsCode, edsConfiguration.isUseKeycloak(), edsUrl);

            LOG.debug("Finished Testing DPA for " + odsCode + " -> has DPA = " + hasDpa);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    /*private static void populateExtractDates(boolean testMode, String configurationId) throws Exception {
        LOG.debug("Populating Extract Dates for " + configurationId + " test mode = " + testMode);
        try {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                conn = ConnectionManager.getSftpReaderNonPooledConnection();

                String sql = null;
                if (ConnectionManager.isPostgreSQL(conn)) {
                    sql = "UPDATE log.batch SET extract_date = ?, extract_cutoff = ? WHERE batch_id = ?";
                } else {
                    sql = "UPDATE batch SET extract_date = ?, extract_cutoff = ? WHERE batch_id = ?";
                }
                ps = conn.prepareStatement(sql);

                DbConfiguration dbConfiguration = null;
                for (DbConfiguration c : configuration.getConfigurations()) {
                    if (c.getConfigurationId().equals(configurationId)) {
                        dbConfiguration = c;
                        break;
                    }
                }
                if (dbConfiguration == null) {
                    throw new Exception("Failed to find configuration " + configurationId);
                }

                SftpBatchDateDetector dateDetector = ImplementationActivator.createSftpDateDetector(dbConfiguration);

                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

                DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
                DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

                DataLayerI dataLayer = configuration.getDataLayer();
                List<Batch> batches = dataLayer.getAllBatches(configurationId);
                LOG.debug("Found " + batches.size() + " batches");

                //ensure batches are sorted properly
                batches.sort((o1, o2) -> {
                    Integer i1 = o1.getSequenceNumber();
                    Integer i2 = o2.getSequenceNumber();
                    return i1.compareTo(i2);
                });

                for (Batch b: batches) {

                    if (!testMode) {
                        if (b.getExtractDate() != null) {
                            LOG.debug("Skipping batch " + b.getBatchId() + ", " + b.getBatchIdentifier() + " as extract date already set");
                            continue;
                        }
                    }

                    String permDir = edsConfiguration.getSharedStoragePath();
                    String tempDir = edsConfiguration.getTempDirectory();
                    String configurationDir = dbConfiguration.getLocalRootPath();
                    String batchDir = b.getLocalRelativePath();

                    String srcDir = FileHelper.concatFilePath(permDir, configurationDir, batchDir);
                    String dstDir = FileHelper.concatFilePath(tempDir, configurationDir, batchDir);

                    //if TPP we need to copy the manifest over
                    if (dateDetector instanceof TppDateDetector) {

                        String fileName = TppConstants.MANIFEST_FILE;
                        String srcPath = FileHelper.concatFilePath(srcDir, fileName);
                        String dstPath = FileHelper.concatFilePath(dstDir, fileName);

                        try {
                            File f = new File(dstPath);
                            if (!f.exists()) {
                                f.mkdirs();
                            }

                            InputStream is = FileHelper.readFileFromSharedStorage(srcPath);
                            Files.copy(is, new File(dstPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                            is.close();
                        } catch (Exception i) {
                            LOG.error("Failed to copy " + srcPath + " to " + dstPath);
                            throw i;
                        }

                    } else if (dateDetector instanceof AdastraDateDetector) {
                        //probably fine as it checks the PERM dir

                    } else if (dateDetector instanceof BartsDateDetector) {
                        //probably fine as it checks the PERM dir

                    } else if (dateDetector instanceof BhrutDateDetector) {
                        //probably fine as it checks the PERM dir

                    } else {
                        //doesn't need any special code
                    }




                    Date extractDate = dateDetector.detectExtractDate(b, configuration.getDataLayer(), edsConfiguration, dbConfiguration);
                    b.setExtractDate(extractDate);

                    Date extractCutoff = dateDetector.detectExtractCutoff(b, configuration.getDataLayer(), edsConfiguration, dbConfiguration);
                    b.setExtractCutoff(extractCutoff);
                    LOG.debug("Batch " + b.getBatchId() + ", " + b.getBatchIdentifier() + " has extract date " + (extractDate != null ? simpleDateFormat.format(extractDate): "null") + " and cutoff " + (extractCutoff != null ? simpleDateFormat.format(extractCutoff) : "null"));

                    if (!testMode) {
                        int col = 1;
                        if (extractDate == null) {
                            ps.setNull(col++, Types.TIMESTAMP);
                        } else {
                            ps.setTimestamp(col++, new java.sql.Timestamp(extractDate.getTime()));
                        }
                        if (extractCutoff == null) {
                            ps.setNull(col++, Types.TIMESTAMP);
                        } else {
                            ps.setTimestamp(col++, new java.sql.Timestamp(extractCutoff.getTime()));
                        }
                        ps.setInt(col++, b.getBatchId());
                        ps.executeUpdate();
                        conn.commit();
                    }
                }

            } catch (Throwable t) {
                LOG.error("", t);
            } finally {
                if (ps != null) {
                    ps.close();
                }
                conn.close();
            }


            LOG.debug("Finished Populating Extract Dates for " + configurationId + " test mode = " + testMode);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/


    private static void populateHasPatientData(boolean testMode, String configurationId, String odsCodeRegex) throws Exception {
        LOG.debug("Populating Extract Dates for " + configurationId + " test mode = " + testMode);
        try {
            Connection conn = null;
            PreparedStatement ps = null;
            try {
                conn = ConnectionManager.getSftpReaderNonPooledConnection();

                String sql = null;
                if (ConnectionManager.isPostgreSQL(conn)) {
                    sql = "UPDATE log.batch_split SET has_patient_data = ? WHERE batch_split_id = ?";
                } else {
                    sql = "UPDATE batch_split SET has_patient_data = ? WHERE batch_split_id = ?";
                }
                ps = conn.prepareStatement(sql);

                DbConfiguration dbConfiguration = null;
                for (DbConfiguration c : configuration.getConfigurations()) {
                    if (c.getConfigurationId().equals(configurationId)) {
                        dbConfiguration = c;
                        break;
                    }
                }
                if (dbConfiguration == null) {
                    throw new Exception("Failed to find configuration " + configurationId);
                }

                SftpBulkDetector bulkDetector = ImplementationActivator.createSftpBulkDetector(dbConfiguration);

                DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
                DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();


                DataLayerI dataLayer = configuration.getDataLayer();
                List<Batch> batches = dataLayer.getAllBatches(configurationId);
                LOG.debug("Found " + batches.size() + " batches");

                //ensure batches are sorted properly
                batches.sort((o1, o2) -> {
                    Integer i1 = o1.getSequenceNumber();
                    Integer i2 = o2.getSequenceNumber();
                    return i1.compareTo(i2);
                });

                for (Batch b: batches) {

                    List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                    LOG.debug(">>>>>>>>Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                    for (BatchSplit split: splits) {

                        String odsCode = split.getOrganisationId();
                        if (!Strings.isNullOrEmpty(odsCodeRegex)
                                && (Strings.isNullOrEmpty(odsCode)
                                || !Pattern.matches(odsCodeRegex, odsCode))) {
                            LOG.debug("Skipping " + odsCode + " due to regex");
                            continue;
                        }
                        LOG.debug("Doing " + odsCode);

                        boolean hasPatientData = false;

                        //if TPP we need to copy the manifest over
                        if (bulkDetector instanceof AdastraBulkDetector) {

                            Set<String> fileTypeIds = new HashSet<>();
                            fileTypeIds.add(AdastraConstants.FILE_ID_PATIENT);
                            fileTypeIds.add(AdastraConstants.FILE_ID_CASE);

                            for (String fileTypeId : fileTypeIds) {

                                String path = AdastraHelper.findPostSplitFileInPermDir(edsConfiguration, dbConfiguration, split, fileTypeId);
                                if (!Strings.isNullOrEmpty(path)) {
                                    //the Adastra files don't include the headers, so we need to call this fn to work it out
                                    CSVFormat csvFormat = AdastraHelper.getCsvFormat(fileTypeId);
                                    boolean isEmpty = SftpBulkDetector.isFileEmpty(path, csvFormat);
                                    if (!isEmpty) {
                                        hasPatientData = true;
                                        break;
                                    }
                                }
                            }

                        } else if (bulkDetector instanceof BartsBulkDetector) {
                            hasPatientData = bulkDetector.hasPatientData(b, split, dataLayer, edsConfiguration, dbConfiguration);

                        } else if (bulkDetector instanceof BhrutBulkDetector) {
                            hasPatientData = bulkDetector.hasPatientData(b, split, dataLayer, edsConfiguration, dbConfiguration);

                        } else if (bulkDetector instanceof EmisBulkDetector) {

                            Set<String> fileTypeIds = new HashSet<>();
                            fileTypeIds.add(EmisConstants.ADMIN_PATIENT_FILE_TYPE);
                            fileTypeIds.add(EmisConstants.CARE_RECORD_CONSULTATION_FILE_TYPE);
                            fileTypeIds.add(EmisConstants.CARE_RECORD_OBSERVATION_FILE_TYPE);
                            fileTypeIds.add(EmisConstants.PRESCRIBING_ISSUE_RECORD);

                            for (String fileTypeId: fileTypeIds) {

                                String path = EmisHelper.findPostSplitFileInPermanentDir(dataLayer, edsConfiguration, dbConfiguration, b, split.getOrganisationId(), fileTypeId);
                                if (!Strings.isNullOrEmpty(path)) {
                                    boolean isEmpty = SftpBulkDetector.isFileEmpty(path, EmisConstants.CSV_FORMAT.withHeader());
                                    if (!isEmpty) {
                                        hasPatientData = true;
                                        break;
                                    }
                                }
                            }

                        } else if (bulkDetector instanceof EmisCustomBulkDetector) {
                            String tempStoragePath = edsConfiguration.getSharedStoragePath(); //bucket
                            String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS_CUSTOM
                            String batchSplitPath = split.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}
                            String permDirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchSplitPath);
                            List<String> filePaths = FileHelper.listFilesInSharedStorage(permDirPath);
                            for (String filePath: filePaths) {
                                boolean isEmpty = SftpBulkDetector.isFileEmpty(filePath, EmisCustomConstants.CSV_FORMAT.withHeader());
                                if (!isEmpty) {
                                    hasPatientData = true;
                                    break;
                                }
                            }


                        } else if (bulkDetector instanceof TppBulkDetector) {
                            Set<String> fileTypeIds = new HashSet<>();
                            fileTypeIds.add(TppConstants.FILE_ID_PATIENT);
                            fileTypeIds.add(TppConstants.FILE_ID_EVENT);
                            fileTypeIds.add(TppConstants.FILE_ID_CODE);

                            for (String fileTypeId: fileTypeIds) {

                                String path = TppHelper.findPostSplitFileInPermDir(edsConfiguration, dbConfiguration, split, fileTypeId);
                                if (!Strings.isNullOrEmpty(path)) {
                                    boolean isEmpty = SftpBulkDetector.isFileEmpty(path, TppConstants.CSV_FORMAT.withHeader());
                                    if (!isEmpty) {
                                        hasPatientData = true;
                                        break;
                                    }
                                }
                            }

                        } else if (bulkDetector instanceof VisionBulkDetector) {
                            Set<String> fileTypeIds = new HashSet<>();
                            fileTypeIds.add(VisionConstants.FILE_ID_PATIENT);
                            fileTypeIds.add(VisionConstants.FILE_ID_JOURNAL);

                            for (String fileTypeId: fileTypeIds) {

                                String path = VisionHelper.findFileInPermDir(edsConfiguration, dbConfiguration, split, fileTypeId);
                                if (!Strings.isNullOrEmpty(path)) {
                                    //the Vision files don't include the headers, so we need to call this fn to work it out
                                    CSVFormat csvFormat = VisionHelper.getCsvFormat(fileTypeId);
                                    boolean isEmpty = SftpBulkDetector.isFileEmpty(path, csvFormat);
                                    if (!isEmpty) {
                                        hasPatientData = true;
                                        break;
                                    }
                                }
                            }

                        } else {
                            throw new Exception("TODO " + bulkDetector.getClass());
                        }

                        LOG.debug("Batch " + b.getBatchId() + ", " + b.getBatchIdentifier() + " and split " + split.getBatchSplitId() + " has patient data = " + hasPatientData);

                        if (!testMode) {
                            ps.setBoolean(1, hasPatientData);
                            ps.setInt(2, split.getBatchSplitId());
                            ps.executeUpdate();
                            conn.commit();
                        }
                    }
                }

            } catch (Throwable t) {
                LOG.error("", t);
            } finally {
                if (ps != null) {
                    ps.close();
                }
                conn.close();
            }


            LOG.debug("Finished Populating Extract Dates for " + configurationId + " test mode = " + testMode);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    /*private static void fixTppOutOfOrderData(String configurationId, boolean testMode, String odsCodeRegex) throws Exception {
        LOG.debug("fixTppOutOfOrderData for " + configurationId + " testmode = " + testMode + " org regex " + odsCodeRegex);

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ConnectionManager.getSftpReaderNonPooledConnection();

            *//*String sql = null;
            if (ConnectionManager.isPostgreSQL(conn)) {
                sql = "UPDATE log.batch_split SET is_bulk = ? WHERE batch_split_id = ?";
            } else {
                sql = "UPDATE batch_split SET is_bulk = ? WHERE batch_split_id = ?";
            }
            ps = conn.prepareStatement(sql);*//*


            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            DataLayerI dataLayer = configuration.getDataLayer();
            List<Batch> batches = dataLayer.getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            //ensure batches are sorted properly
            batches.sort((o1, o2) -> {
                Integer i1 = o1.getSequenceNumber();
                Integer i2 = o2.getSequenceNumber();
                return i1.compareTo(i2);
            });

            //find all orgs to process
            LOG.debug("Finding orgs in batches");
            Map<String, List<Batch>> hmBatchesByOrg = new HashMap<>();
            Map<String, Batch> hmLastBulkByOrg = new HashMap<>();
            Map<Batch, List<BatchSplit>> hmSplitsByBatch = new HashMap<>();

            for (Batch b: batches) {
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                hmSplitsByBatch.put(b, splits);

                for (BatchSplit split: splits) {

                    String odsCode = split.getOrganisationId();
                    if (!Strings.isNullOrEmpty(odsCodeRegex)
                            && (Strings.isNullOrEmpty(odsCode)
                            || !Pattern.matches(odsCodeRegex, odsCode))) {
                        //LOG.debug("Skipping " + odsCode + " due to regex");
                        continue;
                    }

                    List<Batch> l = hmBatchesByOrg.get(odsCode);
                    if (l == null) {
                        l = new ArrayList<>();
                        hmBatchesByOrg.put(odsCode, l);
                    }
                    l.add(b);

                    if (split.isBulk()) {
                        hmLastBulkByOrg.put(odsCode, b);
                    }
                }
            }
            LOG.debug("Found " + hmBatchesByOrg.size() + " orgs");

            for (String odsCode: hmBatchesByOrg.keySet()) {
                batches = hmBatchesByOrg.get(odsCode);
                LOG.debug(">>>>>>>>>>>>>> Doing " + odsCode + " with " + batches.size() + " batches");

                Batch lastBulk = hmLastBulkByOrg.get(odsCode);
                if (lastBulk == null) {
                    LOG.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    LOG.error("No bulk found for " + odsCode);
                    LOG.error("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                    continue;
                }
                LOG.debug("Latest bulk " + lastBulk.getBatchId() + " from " + lastBulk.getBatchIdentifier());

                int lastBulkIndex = batches.indexOf(lastBulk);
                if (lastBulkIndex == -1) {
                    throw new Exception("Failed to find bulk batch " + lastBulk.getBatchId() + " in batches for org");
                }

                List<String> logging = new ArrayList<>();
                logging.add("");

                Date lastDataDate = null;

                for (int i=lastBulkIndex; i<batches.size(); i++) {
                    Batch b = batches.get(i);

                    BatchSplit batchSplit = null;
                    List<BatchSplit> splits = hmSplitsByBatch.get(b);
                    for (BatchSplit s: splits) {
                        if (s.getOrganisationId().equals(odsCode)) {
                            batchSplit = s;
                            break;
                        }
                    }
                    if (batchSplit == null) {
                        throw new Exception("Failed to find batch split in batch " + b.getBatchId());
                    }

                    //find data date
                    Date dataDate = null;
                    boolean foundPatientOrCode = false;

                    //work out S3 path
                    String permDir = edsConfiguration.getSharedStoragePath();
                    String configurationDir = dbConfiguration.getLocalRootPath();
                    String batchSplitRelativePath = batchSplit.getLocalRelativePath();

                    String path = FilenameUtils.concat(permDir, configurationDir);
                    path = FilenameUtils.concat(path, batchSplitRelativePath);

                    List<FileInfo> files = FileHelper.listFilesInSharedStorageWithInfo(path);
                    for (FileInfo fileInfo: files) {

                        String filePath = fileInfo.getFilePath();
                        Date lastModified = fileInfo.getLastModified();
                        long size = fileInfo.getSize();

                        LocalDateTime ldt = LocalDateTime.ofInstant(lastModified.toInstant(), ZoneId.systemDefault());
                        RemoteFile r = new RemoteFile(filePath, size, ldt);

                        SftpFilenameParser parser = ImplementationActivator.createFilenameParser(false, r, dbConfiguration);
                        if (parser.isFilenameValid()) {

                            String fileType = parser.generateFileTypeIdentifier();
                            if (fileType.equals("Code")
                                    || fileType.equals("Patient")) {
                                foundPatientOrCode = true;
                            }

                            Date extractDate = parser.getExtractDate();

                            if (dataDate == null) {
                                dataDate = extractDate;
                            } else if (!dataDate.equals(extractDate)) {
                                throw new Exception("Files in batch " + b.getBatchId() + " have different dates (" + filePath + ")");
                            }
                        }
                    }

                    if (dataDate == null) {
                        throw new Exception("Failed to find data date for batch " + b.getBatchId());
                    }

                    //only count the batch if it had a patient or code file
                    if (foundPatientOrCode) {

                        logging.add("Seq# " + b.getSequenceNumber() + " -> " + simpleDateFormat.format(dataDate) + " batch ID " + b.getBatchId());

                        if (lastDataDate != null
                                && dataDate.before(lastDataDate)) {

                            if (testMode) {
                                logging.add(">>>>>>>>>>Latest batch before previous one!<<<<<<<<<<<<<<");
                                LOG.debug(String.join("\n", logging));

                            } else {
                                //TODO
                            }
                        }

                        lastDataDate = dataDate;

                    }
                }
            }

            LOG.debug("FINISHED fixTppOutOfOrderData for " + configurationId + " testmode = " + testMode + " org regex " + odsCodeRegex);
        } catch (Throwable t) {
            LOG.error("", t);
        } finally {
            if (ps != null) {
                ps.close();
            }
            conn.close();
        }
    }*/

    /*private static void testVisionSlackAlert() {
        try {
            LOG.debug("Testing Vision Slack Alert");

            ConfigurationPollingAttempt newAttempt;
            ConfigurationPollingAttempt previousAttempt;

            String err = "org.endeavourhealth.sftpreader.model.exceptions.SftpReaderException: Exception occurred while downloading files - cannot continue or may process batches out of order\n" +
                    "\tat org.endeavourhealth.sftpreader.SftpReaderTask.downloadNewFiles(SftpReaderTask.java:375)\n" +
                    "\tat org.endeavourhealth.sftpreader.SftpReaderTask.run(SftpReaderTask.java:74)\n" +
                    "\tat java.lang.Thread.run(Thread.java:748)\n" +
                    "Caused by: com.jcraft.jsch.JSchException: Session.connect: java.net.SocketException: Connection reset\n" +
                    "\tat com.jcraft.jsch.Session.connect(Session.java:565)\n" +
                    "\tat com.jcraft.jsch.Session.connect(Session.java:183)\n" +
                    "\tat org.endeavourhealth.sftpreader.sources.SftpConnection.open(SftpConnection.java:74)\n" +
                    "\tat org.endeavourhealth.sftpreader.SftpReaderTask.openSftpConnection(SftpReaderTask.java:391)\n" +
                    "\tat org.endeavourhealth.sftpreader.SftpReaderTask.downloadNewFiles(SftpReaderTask.java:323)\n" +
                    "\t... 2 more\n";

            newAttempt = new ConfigurationPollingAttempt();
            newAttempt.setConfigurationId("VISION_Y03296");
            newAttempt.setErrorText(err);

            previousAttempt = new ConfigurationPollingAttempt();
            previousAttempt.setConfigurationId("VISION_Y03296");
            previousAttempt.setErrorText(null);

            boolean shouldSendAlert = SftpReaderTask.shouldSendSlackAlert(newAttempt, previousAttempt);
            LOG.debug("Would send alert = " + shouldSendAlert + " SHOULD BE FALSE");

            newAttempt = new ConfigurationPollingAttempt();
            newAttempt.setConfigurationId("VISION_Y03296");
            newAttempt.setErrorText(err);

            previousAttempt = new ConfigurationPollingAttempt();
            previousAttempt.setConfigurationId("VISION_Y03296");
            previousAttempt.setErrorText(err);

            shouldSendAlert = SftpReaderTask.shouldSendSlackAlert(newAttempt, previousAttempt);
            LOG.debug("Would send alert = " + shouldSendAlert + " SHOULD BE TRUE");

            LOG.debug("Finished Testing Vision Slack Alert");
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /**
     * TPP manifest files weren't originally copied into the /SPLIT/ODScode directories, but they are now.
     * This routine copies them over where necessary so the old files are in the same pattern as the new
     */
    /*private static void copyTppManifestFiles(String configurationId, boolean testMode) {
        LOG.info("Copying TPP Manifest Files for " + configurationId);

        try {

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            SftpBulkDetector bulkDetector = ImplementationActivator.createSftpBulkDetector(dbConfiguration);

            DataLayerI dataLayer = configuration.getDataLayer();
            List<Batch> batches = dataLayer.getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            //ensure batches are sorted properly
            batches.sort((o1, o2) -> {
                Integer i1 = o1.getSequenceNumber();
                Integer i2 = o2.getSequenceNumber();
                return i1.compareTo(i2);
            });

            for (Batch b: batches) {
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug("Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                String tempDir = edsConfiguration.getTempDirectory(); //e.g. c:\temp
                String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
                String batchRelativePath = b.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00

                String tempPath = FilenameUtils.concat(tempDir, configurationDir);
                tempPath = FilenameUtils.concat(tempPath, batchRelativePath);
                if (!new File(tempPath).exists()) {
                    new File(tempPath).mkdirs();
                }
                String fileTempPath = FilenameUtils.concat(tempPath, TppConstants.MANIFEST_FILE);

                String permPath = FilenameUtils.concat(permDir, configurationDir);
                permPath = FilenameUtils.concat(permPath, batchRelativePath);
                String filePermPath = FilenameUtils.concat(permPath, TppConstants.MANIFEST_FILE);

                try {
                    InputStream is = FileHelper.readFileFromSharedStorage(filePermPath);
                    Files.copy(is, new File(fileTempPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                    is.close();
                } catch (Exception ex) {
                    LOG.error("Error copying " + filePermPath + " to " + fileTempPath);
                    throw ex;
                }

                //now check to see if the file exists in the SPLIT dir for each org
                for (BatchSplit split: splits) {

                    String splitRelativePath = split.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

                    String orgPermPath = FilenameUtils.concat(permDir, configurationDir);
                    orgPermPath = FilenameUtils.concat(orgPermPath, splitRelativePath);

                    boolean manifestMissing = true;

                    List<String> files = FileHelper.listFilesInSharedStorage(orgPermPath);
                    for (String file: files) {
                        String fileName = FilenameUtils.getName(file);
                        if (fileName.equals(TppConstants.MANIFEST_FILE)) {
                            manifestMissing = false;
                            break;
                        }
                    }

                    if (manifestMissing) {

                        if (testMode) {
                            LOG.info("Would need to fix " + orgPermPath);

                        } else {
                            String orgPermFile = FilenameUtils.concat(orgPermPath, TppConstants.MANIFEST_FILE);
                            FileHelper.writeFileToSharedStorage(orgPermFile, new File(fileTempPath));

                            LOG.info("Copied into " + orgPermPath);
                        }
                    }
                }

                //delete the tmp directory contents
                FileHelper.deleteRecursiveIfExists(tempPath);
            }

            LOG.info("Finished checking for Bulks in " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /**
     * recreates the batch_file table content after it was lost, but for the
     * Emis configurations where the raw GPG files are deleted from S3 after four weeks
     */
    /*private static void recreateBatchFile2(String configurationId) {
        LOG.info("Recreating batch file <2> for " + configurationId);
        try {

            if (!configurationId.contains("EMIS")) {
                throw new Exception("Cannot run for non-Emis configuration");
            }

            Connection conn = ConnectionManager.getSftpReaderNonPooledConnection();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            Date now = new Date();

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            List<Batch> batches = configuration.getDataLayer().getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            String sql = null;
            if (ConnectionManager.isPostgreSQL(conn)) {
                sql = "SELECT interface_type_id FROM configuration.configuration WHERE configuration_id = ?";
            } else {
                sql = "SELECT interface_type_id FROM configuration WHERE configuration_id = ?";
            }
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();
            rs.next();
            int interfaceTypeId = rs.getInt(1);

            ps.close();

            String localPath = dbConfiguration.getLocalRootPath();

            for (int i=0; i<batches.size(); i++) {
                Batch batch = batches.get(i);

                List<BatchSplit> batchSplits = configuration.getDataLayer().getBatchSplitsForBatch(batch.getBatchId());
                if (batchSplits.isEmpty()) {
                    throw new Exception("No batch splits for batch " + batch.getBatchId());
                }

                Date batchCreateDate = batch.getInsertDate();

                Map<String, String> hmFileNamesByType = new HashMap<>();
                Map<String, Date> hmFileDatesByType = new HashMap<>();

                for (BatchSplit batchSplit: batchSplits) {

                    //work out S3 path
                    String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                    String configurationDir = localPath; //e.g. TPP_TEST
                    String batchSplitRelativePath = batchSplit.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00/Split/<org guid>/

                    String path = FilenameUtils.concat(permDir, configurationDir);
                    path = FilenameUtils.concat(path, batchSplitRelativePath);

                    List<FileInfo> files = FileHelper.listFilesInSharedStorageWithInfo(path);
                    for (FileInfo fileInfo: files) {

                        String filePath = fileInfo.getFilePath();
                        Date lastModified = fileInfo.getLastModified();
                        long size = fileInfo.getSize();

                        LocalDateTime ldt = LocalDateTime.ofInstant(lastModified.toInstant(), ZoneId.systemDefault());
                        RemoteFile r = new RemoteFile(filePath, size, ldt);

                        SftpFilenameParser parser = ImplementationActivator.createFilenameParser(false, r, dbConfiguration);
                        if (parser.isFilenameValid()) {

                            String fileType = parser.generateFileTypeIdentifier();
                            Date extractDate = parser.getExtractDate();
                            String fileName = FilenameUtils.getName(filePath) + ".gpg"; //files in split dirs have had this removed

                            hmFileNamesByType.put(fileType, fileName);
                            hmFileDatesByType.put(fileType, extractDate);
                        }
                    }
                }

                for (String fileType: hmFileDatesByType.keySet()) {
                    String fileName = hmFileNamesByType.get(fileType);
                    Date extractDate = hmFileDatesByType.get(fileType);

                    //check batch file
                    boolean alreadyThere = false;
                    for (BatchFile batchFile: batch.getBatchFiles()) {
                        if (batchFile.getFileTypeIdentifier().equals(fileType)) {
                            alreadyThere = true;
                            break;
                        }
                    }
                    if (alreadyThere) {
                        continue;
                    }

                    if (ConnectionManager.isPostgreSQL(conn)) {
                        sql = "INSERT INTO log.batch_file (batch_id, interface_type_id, file_type_identifier,"
                                + " insert_date, filename, remote_created_date, remote_size_bytes, is_downloaded, download_date, is_deleted)"
                                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    } else {
                        sql = "INSERT INTO batch_file (batch_id, interface_type_id, file_type_identifier,"
                                + " insert_date, filename, remote_created_date, remote_size_bytes, is_downloaded, download_date, is_deleted)"
                                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                    }
                    ps = conn.prepareStatement(sql);

                    int col = 1;
                    ps.setInt(col++, batch.getBatchId()); //batch_id
                    ps.setInt(col++, interfaceTypeId); //interface_type_id
                    ps.setString(col++, fileType); //file_type_identifier
                    ps.setDate(col++, new java.sql.Date(now.getTime())); //insert_date
                    ps.setString(col++, fileName); //filename
                    ps.setDate(col++, new java.sql.Date(extractDate.getTime())); //remote_created_date
                    ps.setLong(col++, 0); //remote_size_bytes
                    ps.setBoolean(col++, true); //is_downloaded
                    ps.setDate(col++, new java.sql.Date(batchCreateDate.getTime())); //download_date
                    ps.setBoolean(col++, true); //is_deleted

                    ps.executeUpdate();
                    conn.commit();
                    ps.close();
                }

                if (i % 100 == 0) {
                    LOG.debug("Done " + i + " of " + batches.size());
                }
            }

            conn.close();

            LOG.info("Finished recreating batch file");
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /**
     * recreates the batch_file table content after it was lost using what's in S3
     */
    /*private static void recreateBatchFile(String configurationId) {
        LOG.info("Recreating batch file for " + configurationId);
        try {
            Connection conn = ConnectionManager.getSftpReaderNonPooledConnection();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            Date now = new Date();

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            List<Batch> batches = configuration.getDataLayer().getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            String sql = "SELECT interface_type_id FROM configuration.configuration WHERE configuration_id = ?";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setString(1, configurationId);

            ResultSet rs = ps.executeQuery();
            rs.next();
            int interfaceTypeId = rs.getInt(1);

            ps.close();

            String localPath = dbConfiguration.getLocalRootPath();

            for (int i=0; i<batches.size(); i++) {
                Batch batch = batches.get(i);

                //work out S3 path
                String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                String configurationDir = localPath; //e.g. TPP_TEST
                String batchRelativePath = batch.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00

                String path = FilenameUtils.concat(permDir, configurationDir);
                path = FilenameUtils.concat(path, batchRelativePath);

                Date batchCreateDate = batch.getInsertDate();

                List<FileInfo> files = FileHelper.listFilesInSharedStorageWithInfo(path);
                for (FileInfo fileInfo: files) {

                    String filePath = fileInfo.getFilePath();
                    Date lastModified = fileInfo.getLastModified();
                    long size = fileInfo.getSize();
                    String fileName = FilenameUtils.getName(filePath);

                    //ignore anything not in the directory itself
                    //LOG.debug("Split file = " + filePath.contains("/Split/") + " for " + filePath);
                    if (filePath.contains("/Split/")) {
                        continue;
                    }

                    LocalDateTime ldt = LocalDateTime.ofInstant(lastModified.toInstant(), ZoneId.systemDefault());
                    RemoteFile r = new RemoteFile(filePath, size, ldt);

                    boolean isRaw = true;

                    //TPP filename parser needs to think it's post-processed files so it can handle the directory names in S3
                    String contentType = dbConfiguration.getSoftwareContentType();
                    if (contentType.equalsIgnoreCase("TPPCSV")) {
                        isRaw = false;
                    }

                    SftpFilenameParser parser = ImplementationActivator.createFilenameParser(isRaw, r, dbConfiguration);
                    if (parser.isFilenameValid()) {
                        String fileType = parser.generateFileTypeIdentifier();
                        Date extractDate = parser.getExtractDate();

                        sql = "INSERT INTO log.batch_file (batch_id, interface_type_id, file_type_identifier,"
                                + " insert_date, filename, remote_created_date, remote_size_bytes, is_downloaded, download_date, is_deleted)"
                                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        ps = conn.prepareStatement(sql);

                        int col = 1;
                        ps.setInt(col++, batch.getBatchId()); //batch_id
                        ps.setInt(col++, interfaceTypeId); //interface_type_id
                        ps.setString(col++, fileType); //file_type_identifier
                        ps.setDate(col++, new java.sql.Date(now.getTime())); //insert_date
                        ps.setString(col++, fileName); //filename
                        ps.setDate(col++, new java.sql.Date(extractDate.getTime())); //remote_created_date
                        ps.setLong(col++, size); //remote_size_bytes
                        ps.setBoolean(col++, true); //is_downloaded
                        ps.setDate(col++, new java.sql.Date(batchCreateDate.getTime())); //download_date
                        ps.setBoolean(col++, false); //is_deleted

                        ps.executeUpdate();
                        conn.commit();
                        ps.close();

                    } else {
                        LOG.debug("Skipping file " + filePath);
                    }

                }

                if (i % 100 == 0) {
                    LOG.debug("Done " + i + " of " + batches.size());
                }
            }

            conn.close();

            LOG.info("Finished recreating batch file");
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /**
     * one-off routine to populate the new is_bulk column on the batch_split table so we
     * can work out if we've received bulk data for a service or not
     */
    private static void checkForBulks(String configurationId, boolean testMode, String odsCodeRegex) throws Exception {
        LOG.info("Checking for Bulks in " + configurationId);
        if (!Strings.isNullOrEmpty(odsCodeRegex)) {
            LOG.info("Restricting to orgs matching " + odsCodeRegex);
        }

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ConnectionManager.getSftpReaderNonPooledConnection();

            String sql = null;
            if (ConnectionManager.isPostgreSQL(conn)) {
                sql = "UPDATE log.batch_split SET is_bulk = ? WHERE batch_split_id = ?";
            } else {
                sql = "UPDATE batch_split SET is_bulk = ? WHERE batch_split_id = ?";
            }
            ps = conn.prepareStatement(sql);


            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            SftpBulkDetector bulkDetector = ImplementationActivator.createSftpBulkDetector(dbConfiguration);

            DataLayerI dataLayer = configuration.getDataLayer();
            List<Batch> batches = dataLayer.getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            //ensure batches are sorted properly
            batches.sort((o1, o2) -> {
                Integer i1 = o1.getSequenceNumber();
                Integer i2 = o2.getSequenceNumber();
                return i1.compareTo(i2);
            });

            for (Batch b: batches) {
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug(">>>>>>>>Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {

                    String odsCode = split.getOrganisationId();
                    if (!Strings.isNullOrEmpty(odsCodeRegex)
                            && (Strings.isNullOrEmpty(odsCode)
                                || !Pattern.matches(odsCodeRegex, odsCode))) {
                        //LOG.debug("Skipping " + odsCode + " due to regex");
                        continue;
                    }
                    LOG.debug("Doing " + odsCode);

                    String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                    String tempDir = edsConfiguration.getTempDirectory(); //e.g. c:\temp
                    String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
                    String batchRelativePath = b.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00
                    String splitRelativePath = split.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

                    String tempPath = FilenameUtils.concat(tempDir, configurationDir);
                    tempPath = FilenameUtils.concat(tempPath, splitRelativePath);
                    boolean createdTempPath = new File(tempPath).mkdirs();

                    //LOG.debug(">>>>>>>>>>>>>>>>>>>>>>>Doing batch split " + split.getBatchSplitId());

                    if (bulkDetector instanceof TppBulkDetector) {
                        //for TPP, we have to copy the Manifest, Patient and Code file to tmp
                        List<String> files = new ArrayList<>();
                        files.add(TppConstants.MANIFEST_FILE);
                        files.add(TppConstants.PATIENT_FILE);
                        files.add(TppConstants.CODE_FILE);

                        String permPath = FilenameUtils.concat(permDir, configurationDir);
                        permPath = FilenameUtils.concat(permPath, splitRelativePath);

                        List<String> storageContents = FileHelper.listFilesInSharedStorage(permPath);
                        for (String filePermPath: storageContents) {
                            String fileName = FilenameUtils.getName(filePermPath);
                            if (files.contains(fileName)) {

                                String fileTempPath = FilenameUtils.concat(tempPath, fileName);

                                try {
                                    InputStream is = FileHelper.readFileFromSharedStorage(filePermPath);
                                    Files.copy(is, new File(fileTempPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                                    is.close();
                                    //LOG.trace("Copied " + filePermPath + " to " + fileTempPath);
                                } catch (Exception ex) {
                                    LOG.error("Error copying " + filePermPath + " to " + fileTempPath);
                                    throw ex;
                                }
                            }
                        }

                    } else if (bulkDetector instanceof VisionBulkDetector) {
                        //for Vision, we need the patient and journal files
                        String permPatientPath = null;
                        String permJournalPath = null;

                        String permPath = FilenameUtils.concat(permDir, configurationDir);
                        permPath = FilenameUtils.concat(permPath, batchRelativePath); //don't have this in the split path
                        List<String> files = FileHelper.listFilesInSharedStorage(permPath);
                        for (String file: files) {
                            String ext = FilenameUtils.getExtension(file);
                            boolean isRawFile = ext.equalsIgnoreCase("zip");
                            RemoteFile r = new RemoteFile(file, -1, null);
                            VisionFilenameParser p = new VisionFilenameParser(isRawFile, r, dbConfiguration);
                            String fileType = p.generateFileTypeIdentifier();
                            if (fileType.equals(VisionConstants.FILE_ID_PATIENT)) {
                                permPatientPath = file;
                            } else if (fileType.equals(VisionConstants.FILE_ID_JOURNAL)) {
                                permJournalPath = file;
                            }
                        }

                        //got some Vision deltas which didn't contain all files
                        if (permPatientPath != null) {
                            String patientFileName = FilenameUtils.getName(permPatientPath);
                            String tmpPatientPath = FilenameUtils.concat(tempPath, patientFileName);

                            InputStream is = FileHelper.readFileFromSharedStorage(permPatientPath);
                            Files.copy(is, new File(tmpPatientPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                            is.close();
                        }

                        if (permJournalPath != null) {
                            String journalFileName = FilenameUtils.getName(permJournalPath);
                            String tmpJournalPath = FilenameUtils.concat(tempPath, journalFileName);

                            InputStream is = FileHelper.readFileFromSharedStorage(permJournalPath);
                            Files.copy(is, new File(tmpJournalPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                            is.close();
                        }

                    } else if (bulkDetector instanceof EmisBulkDetector) {
                        //for Emis, we need the patient and observation files
                        String permPatientPath = EmisHelper.findPostSplitFileInPermanentDir(dataLayer, edsConfiguration, dbConfiguration, b, split.getOrganisationId(), EmisConstants.ADMIN_PATIENT_FILE_TYPE);
                        String permObservationPath = EmisHelper.findPostSplitFileInPermanentDir(dataLayer, edsConfiguration, dbConfiguration, b, split.getOrganisationId(), EmisConstants.CARE_RECORD_OBSERVATION_FILE_TYPE);

                        String patientFileName = FilenameUtils.getName(permPatientPath);
                        String observationFileName = FilenameUtils.getName(permObservationPath);

                        if (!new File(tempPath).exists()) {
                            LOG.error("Temp path " + tempPath + " doesn't exist");
                            boolean created = new File(tempPath).mkdirs();
                            LOG.debug("Created Temp path = " + created);
                        }

                        String tmpPatientPath = FilenameUtils.concat(tempPath, patientFileName);
                        String tmpObservationPath = FilenameUtils.concat(tempPath, observationFileName);

                        LOG.debug("Copying patient file from " + permPatientPath + " -> " + tmpPatientPath);
                        InputStream is = FileHelper.readFileFromSharedStorage(permPatientPath);
                        Files.copy(is, new File(tmpPatientPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        is.close();

                        LOG.debug("Copying observation file from " + permObservationPath + " -> " + tmpObservationPath);
                        is = FileHelper.readFileFromSharedStorage(permObservationPath);
                        Files.copy(is, new File(tmpObservationPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        is.close();


                    } else {
                        throw new Exception("Not implemented this yet for " + bulkDetector.getClass().getSimpleName());
                    }

                    try {
                        boolean isBulk = bulkDetector.isBulkExtract(b, split, dataLayer, edsConfiguration, dbConfiguration);

                        if (isBulk) {
                            LOG.debug("    Found bulk for " + split.getOrganisationId());
                        }

                        if (testMode) {
                            LOG.debug("" + odsCode + " bulk = " + isBulk);

                        } else {
                            ps.setBoolean(1, isBulk);
                            ps.setInt(2, split.getBatchSplitId());

                            ps.executeUpdate();
                            conn.commit();
                        }

                    } catch (Exception ex) {
                        if (testMode) {
                            LOG.error(ex.getMessage());
                        } else {
                            throw ex;
                        }
                    }

                    //delete the tmp directory contents
                    FileHelper.deleteRecursiveIfExists(tempPath);
                }
            }

            LOG.info("Finished checking for Bulks in " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }  finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /*private static void testSplitting(String srcFilePath) {
        LOG.info("Testing splitting of " + srcFilePath);
        try {
            File srcFile = new File(srcFilePath);
            String srcDir = srcFile.getParent();
            String dst = FilenameUtils.concat(srcDir, "Split");
            File dstDir = new File(dst);

            CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader("col1","col2","col3","col4");

            CsvSplitter splitter = new CsvSplitter(srcFilePath, dstDir, false, csvFormat, "col1");
            List<File> files = splitter.go();
            LOG.info("Done split from " + srcFile);

            String targetfileName = "COMBINED_" + srcFile.getName();
            String targetFilePathStr = FilenameUtils.concat(srcDir, targetfileName);
            File targetFile = new File(targetFilePathStr);
            CsvJoiner joiner = new CsvJoiner(files, targetFile, csvFormat);
            joiner.go();
            LOG.info("Done join to " + targetFile);

            LOG.info("Finished Testing splitting of " + srcFilePath);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    private static void decryptGpgFile(String configurationId, String srcFilePath, String dstFilePath) {
        LOG.info("Decrypting " + srcFilePath + " to " + dstFilePath + " from configuration " + configurationId);
        try {
            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            InputStream inputStream = FileHelper.readFileFromSharedStorage(srcFilePath);

            String privateKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKey();
            String privateKeyPassword = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();
            String publicKey = dbConfiguration.getPgpConfiguration().getPgpSenderPublicKey();

            //delete destination file and make sure directories exist
            File dstFile = new File(dstFilePath);
            if (dstFile.exists()) {
                dstFile.delete();
            } else {
                File dstDir = dstFile.getParentFile();
                if (dstDir != null && !dstDir.exists()) {
                    dstDir.mkdirs();
                }
            }

            try {
                LOG.debug("Opened input stream");
                PgpUtil.decryptAndVerify(inputStream, dstFilePath, privateKey, privateKeyPassword, publicKey);

            } finally {
                inputStream.close();
            }

            LOG.info("Finished Decrypting " + srcFilePath + " to " + dstFilePath + " from configuration " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static void readS3Bytes(String path, long start, long len) {
        LOG.info("Reading " + path + " from " + start + " len " + len);
        try {
            String str = FileHelper.readCharactersFromSharedStorage(path, start, len);
            LOG.info("");
            LOG.info(str);
            LOG.info("");
            LOG.info("Read " + str.length());

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static void fixEmisS3Tags(String configurationId) {
        try {
            LOG.debug("Fixing S3 Tags for " + configurationId);

            DataLayerI db = configuration.getDataLayer();

            DbInstance dbInstanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }

            Map<String, String> tags = new HashMap<>();
            tags.put("Emis", "raw");

            List<Batch> batches = db.getAllBatches(configurationId);
            for (Batch batch: batches) {
                LOG.debug("Fixing batch " + batch.getBatchId() + " " + batch.getBatchIdentifier());

                for (BatchFile batchFile: batch.getBatchFiles()) {

                    String sharedStoragePath = edsConfiguration.getSharedStoragePath(); //e.g. s3://discoverysftplanding/endeavour
                    String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS001
                    String batchPath = batch.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/
                    String fileName = batchFile.getFilename(); //e.g. 1-95771_Admin_OrganisationLocation_20170222170437_2D106103-404D-4054-89B8-18F95932B092.csv.gpg

                    String path = FilenameUtils.concat(sharedStoragePath, configurationPath);
                    path = FilenameUtils.concat(path, batchPath);
                    path = FilenameUtils.concat(path, fileName);

                    String extension = FilenameUtils.getExtension(fileName);
                    if (!extension.equalsIgnoreCase("gpg")) {
                        throw new Exception("" + path + " isn't a GPG file");
                    }

                    FileHelper.setPermanentStorageTags(path, tags);
                }
            }

            LOG.debug("Finished Fixing S3 Tags for " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static void testS3Tags(String s3Path, String tag, String tagValue) {

        try {
            LOG.debug("Testing S3 tagging of " + s3Path);

            Map<String, String> hm = new HashMap<>();

            if (!Strings.isNullOrEmpty(tagValue)) {
                hm.put(tag, tagValue);

                FileHelper.setPermanentStorageTags(s3Path, hm);
                LOG.debug("Written tag [" + tag + "] -> [" + tagValue + "]");
            } else {
                LOG.debug("Not writing tag");
            }

            hm = FileHelper.getPermanentStorageTags(s3Path);
            String gotValue = hm.get(tag);
            LOG.debug("Got tag value back [" + gotValue + "]");

            LOG.debug("Finished Testing S3 tagging of " + s3Path);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    private static void testDisabledFix(String odsCode, String configurationId) {

        try {
            LOG.debug("Running test fix for " + odsCode);

            DataLayerI db = configuration.getDataLayer();
            List<EmisOrganisationMap> orgs = db.getEmisOrganisationMapsForOdsCode(odsCode);
            if (orgs.size() > 1) {
                throw new Exception("Found multiple org records for ODS code " + odsCode);
            }
            EmisOrganisationMap org = orgs.get(0);
            LOG.debug("Org = " + org.getName());

            DbInstance dbInstanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = dbInstanceConfiguration.getEdsConfiguration();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }

            EmisFixDisabledService fixer = new EmisFixDisabledService(org, db, edsConfiguration, dbConfiguration);

            fixer.fixDisabledExtract();

            LOG.debug("Finished Running test fix for " + odsCode);
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

    /*private static void testOriginalTerms() {

        try {
            LOG.debug("Testing Original Terms");

            CSVFormat csvFormat = CSVFormat.TDF.withHeader()
                    .withEscape((Character)null)
                    .withQuote((Character)null)
                    .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mdde NONE, but validation in the library means we need to use this


            JFileChooser f = new JFileChooser();
            f.setFileSelectionMode(JFileChooser.FILES_ONLY);
            f.setMultiSelectionEnabled(false);
            f.setCurrentDirectory(new File("C:\\SFTPData\\original_terms"));

            int r = f.showOpenDialog(null);
            if (r == JFileChooser.CANCEL_OPTION) {
                return;
            }


            File srcFile = f.getSelectedFile();
            LOG.debug("Selected " + srcFile + " len " + srcFile.length());
            String unzippedFile = srcFile.getAbsolutePath();




            //The original terms is a tab-separated file that doesn't include any quoting of text fields, however there are a number
            //of records that contain new-line characters, meaning the CSV parser can't handle those records
            //So we need to pre-process the file to quote those records so the CSV Parser can handle them
            InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(unzippedFile);
            CSVParser parser = new CSVParser(reader, csvFormat.withHeader());
            Iterator<CSVRecord> iterator = parser.iterator();

            String fixedSrcFile = srcFile + "FIXED";

            FileOutputStream fos = new FileOutputStream(fixedSrcFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            BufferedWriter bufferedWriter = new BufferedWriter(osw);

            Map<String, Integer> headerMap = parser.getHeaderMap();
            String[] headers = new String[headerMap.size()];
            for (String headerVal: headerMap.keySet()) {
                Integer index = headerMap.get(headerVal);
                headers[index.intValue()] = headerVal;
            }

            CSVPrinter printer = new CSVPrinter(bufferedWriter, csvFormat.withHeader(headers));
            printer.flush();

            int fixed = 0;
            int total = 0;
            int written = 0;

            String[] pendingRecord = null;
            while (iterator.hasNext()) {
                CSVRecord next = iterator.next();
                total ++;

                if (next.size() == 5) {
                    //if a valid line, write any pending record and swap this to be our pending record
                    if (pendingRecord != null) {
                        printer.printRecord((Object[])pendingRecord);
                        written ++;
                    }

                    pendingRecord = new String[next.size()];
                    for (int i=0; i<pendingRecord.length; i++) {
                        pendingRecord[i] = next.get(i);
                    }

                } else if (next.size() == 1) {
                    //if one of the invalid lines, then append to the pending record
                    String extraText = next.get(0);
                    String currentText = pendingRecord[pendingRecord.length-1];
                    String combinedText = currentText + ", " + extraText;
                    pendingRecord[pendingRecord.length-1] = combinedText;
                    fixed ++;

                } else {
                    //no idea what this would be, but it's wrong
                    throw new Exception("Failed to handle record " + next + " with " + next.size() + " columns");
                }
            }

            //pring the final record
            if (pendingRecord != null) {
                printer.printRecord((Object[])pendingRecord);
                written ++;
            }

            parser.close();
            printer.close();

            LOG.debug("Total " + total + " fixed " + fixed + " written " + written);

            File dstDir = new File(srcFile.getParentFile(), "Split");
            CsvSplitter csvSplitter = new CsvSplitter(fixedSrcFile, dstDir, csvFormat.withHeader(), "OrganisationOds");
            //CsvSplitter csvSplitter = new CsvSplitter(unzippedFile, dstDir, csvFormat, "OrganisationOds");
            List<File> splitFiles = csvSplitter.go();
            LOG.debug("Finished split into " + splitFiles.size());
            for (File splitFile: splitFiles) {
                LOG.debug("    " + splitFile);
            }

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /*private static void testS3(String[] args) {
        LOG.debug("Testing S3");
        try {
            String src = args[1];
            String dstBucket = args[2];
            String dstKey = args[3];
            String encryption = args[4];

            File srcFile = new File(src);
            if (!srcFile.exists()) {
                throw new Exception("" + src + " doesn't exist");
            }

            AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                    .withRegion(Regions.EU_WEST_2);

            AmazonS3 s3Client = clientBuilder.build();

            ObjectMetadata objectMetadata = new ObjectMetadata();
            if (encryption.equalsIgnoreCase("AES")) {
                objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                LOG.debug("Writing with AES encryption");

            } else if (encryption.equalsIgnoreCase("NONE")) {
                //nothing
                LOG.debug("Writing with no encryption");

            } else if (encryption.equals("KMS")) {

                String s = SSEAlgorithm.KMS.getAlgorithm();
                objectMetadata.setSSEAlgorithm(s);

                LOG.debug("Writing with KMS encryption (" + s + ")");

            } else {
                throw new Exception("Unknown encryption mode");
            }

            PutObjectRequest putRequest = new PutObjectRequest(dstBucket, dstKey, srcFile);
            putRequest.setMetadata(objectMetadata);

            s3Client.putObject(putRequest);

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /*private static void testSplittingAndJoining() {
        try {
            LOG.info("Testing Splitting and Joining");

            JFileChooser f = new JFileChooser();
            f.setFileSelectionMode(JFileChooser.FILES_ONLY);
            f.setMultiSelectionEnabled(false);

            int r = f.showOpenDialog(null);
            if (r == JFileChooser.CANCEL_OPTION) {
                return;
            }

            File src = f.getSelectedFile();
            File dir = src.getParentFile();
            File workingDir = new File(dir, "tmp");
            if (!workingDir.exists()) {
                workingDir.mkdirs();
            }
            CsvSplitter splitter = new CsvSplitter(src.getAbsolutePath(), workingDir, CSVFormat.DEFAULT, "OrganisationGuid");
            List<File> files = splitter.go();
            LOG.info("Done split");

            File dst = new File(workingDir, src.getName());
            CsvJoiner csvJoiner = new CsvJoiner(files, dst, CSVFormat.DEFAULT);
            csvJoiner.go();

            LOG.info("Done join");

            LOG.info("Finished Testing Splitting and Joining");
        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/


    private static void shutdown() {
        try {
            LOG.info("Shutting down...");

            if (sftpReaderTaskScheduler != null)
                sftpReaderTaskScheduler.stop();

        } catch (Exception e) {
            printToErrorConsole("Exception occurred during shutdown", e);
            LOG.error("Exception occurred during shutdown", e);
        }
    }

    private static void printToErrorConsole(String message, Exception e) {
        System.err.println(message + " [" + e.getClass().getName() + "] " + e.getMessage());
    }

    /**
     * simple utility to log what we can find about an S3 object or prefix
     */
    private static void checkS3(String path) throws Exception {
        System.out.println("Checking S3 " + path);

        //if a path, then list contents
        if (path.endsWith("/")) {

            //remove trailing /
            path = path.substring(0, path.length()-1);

            List<FileInfo> infos = FileHelper.listFilesInSharedStorageWithInfo(path);
            System.out.println("Prefix listing:");
            for (FileInfo info: infos) {
                String s = "";

                s += info.getLastModified();
                while (s.length() < 31) {
                    s += " ";
                }

                s += FileUtils.byteCountToDisplaySize(info.getSize());
                while (s.length() < 41) {
                    s += " ";
                }

                s += info.getFilePath();

                System.out.println(s);
            }

        } else {
            //if an object, list tags and encryption status
            /*List<FileInfo> infos = FileHelper.listFilesInSharedStorageWithInfo(path);
            FileInfo info = infos.get(0);

            LOG.info("Last modified: " + info.getLastModified());
            LOG.info("Size: " + FileUtils.byteCountToDisplaySize(info.getSize()));*/

            System.out.println("Tags:");
            Map<String, String> tags = FileHelper.getPermanentStorageTags(path);
            if (tags == null || tags.isEmpty()) {
                System.out.println("    No tags found");
            } else {
                for (String tag: tags.keySet()) {
                    String value = tags.get(tag);
                    System.out.println("    " + tag + " = " + value);
                }
            }

            AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder
                    .standard()
                    .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                    .withRegion(Regions.EU_WEST_2);

            AmazonS3 s3Client = clientBuilder.build();

            int index = path.indexOf("/", 5);
            String bucket = path.substring(0, index);
            String key = path.substring(index+1);

            LOG.debug("Bucket = " + bucket);
            LOG.debug("key = " + key);

            GetObjectMetadataRequest request2 = new GetObjectMetadataRequest(bucket, key);
            ObjectMetadata metadata = s3Client.getObjectMetadata(request2);
            String encryption = metadata.getSSEAlgorithm();
            System.out.println("Encryption: " + encryption);
        }
    }


    /*private static void fixS3(String bucket, String path, boolean test) {
        LOG.info("Fixing S3 " + bucket + " for " + path + " test mode = " + test);

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder
                .standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.EU_WEST_2);

        AmazonS3 s3Client = clientBuilder.build();

        ListObjectsV2Request request = new ListObjectsV2Request();
        request.setBucketName(bucket);
        request.setPrefix(path);

        while (true) {

            ListObjectsV2Result result = s3Client.listObjectsV2(request);
            if (result.getObjectSummaries() != null) {
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String key = objectSummary.getKey();

                    GetObjectMetadataRequest request2 = new GetObjectMetadataRequest(bucket, key);
                    ObjectMetadata metadata = s3Client.getObjectMetadata(request2);
                    String encryption = metadata.getSSEAlgorithm();
                    LOG.info("" + key + " has encryption [" + encryption + "]");

                    if (!test
                        && (Strings.isNullOrEmpty(encryption) || encryption.equalsIgnoreCase("AES256"))) {

                        String key2 = key + "_COPY";

                        ObjectMetadata newObjectMetadata = new ObjectMetadata();
                        String s = SSEAlgorithm.KMS.getAlgorithm();
                        newObjectMetadata.setSSEAlgorithm(s);


                        long len = metadata.getContentLength();
                        long maxChunk = 1024L * 1024L * 1024L * 4L; //4GB
                        if (len > maxChunk) {

                            //copy to backup
                            copyLargeFile(s3Client, bucket, key, key2, maxChunk, null);

                            //copy back to original but WITH encryption
                            copyLargeFile(s3Client, bucket, key2, key, maxChunk, newObjectMetadata);

                        } else {

                            //copy to backup
                            CopyObjectRequest copyRequest = new CopyObjectRequest(bucket, key, bucket, key2);
                            s3Client.copyObject(copyRequest);

                            //copy back to original but WITH encryption
                            copyRequest = new CopyObjectRequest(bucket, key2, bucket, key);
                            copyRequest.setNewObjectMetadata(newObjectMetadata);
                            s3Client.copyObject(copyRequest);
                        }

                        //delete backup
                        DeleteObjectRequest deleteRequest = new DeleteObjectRequest(bucket, key2);
                        s3Client.deleteObject(deleteRequest);

                        LOG.info("Fixed " + key);
                    }
                }
            }

            if (result.isTruncated()) {
                String nextToken = result.getNextContinuationToken();
                request.setContinuationToken(nextToken);
                continue;

            } else {
                break;
            }
        }

        LOG.info("Finished Fixing S3 " + bucket + " for " + path);
    }*/

    /*private static void testCopyLargeFile(String bucket, String srcKey, String dstKey, long maxChunk) {
        LOG.info("Testing copying S3 " + bucket + " from " + srcKey + " to " + dstKey);

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder
                .standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.EU_WEST_2);

        AmazonS3 s3Client = clientBuilder.build();

        copyLargeFile(s3Client, bucket, srcKey, dstKey, maxChunk, null);

        LOG.info("Finishing Testing copying S3 " + bucket + " from " + srcKey + " to " + dstKey);
    }

    private static void copyLargeFile(AmazonS3 s3Client, String bucket, String srcKey, String dstKey, long maxChunk, ObjectMetadata newMetadata) {

        LOG.debug("Copying " + srcKey + " -> " + dstKey + " in " + maxChunk + " chunks");

        // Initiate the multipart upload.
        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, dstKey);
        if (newMetadata != null) {
            initRequest.setObjectMetadata(newMetadata);
        }

        InitiateMultipartUploadResult initResult = s3Client.initiateMultipartUpload(initRequest);

        // Get the object size to track the end of the copy operation.
        GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucket, srcKey);
        ObjectMetadata metadataResult = s3Client.getObjectMetadata(metadataRequest);
        long objectSize = metadataResult.getContentLength();

        // Copy the object using 5 MB parts.
        long bytePosition = 0;
        int partNum = 1;
        List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
        while (bytePosition < objectSize) {
            // The last part might be smaller than partSize, so check to make sure
            // that lastByte isn't beyond the end of the object.
            long lastByte = Math.min(bytePosition + maxChunk - 1, objectSize - 1);

            // Copy this part.
            CopyPartRequest copyRequest = new CopyPartRequest()
                    .withSourceBucketName(bucket)
                    .withSourceKey(srcKey)
                    .withDestinationBucketName(bucket)
                    .withDestinationKey(dstKey)
                    .withUploadId(initResult.getUploadId())
                    .withFirstByte(bytePosition)
                    .withLastByte(lastByte)
                    .withPartNumber(partNum++);

            copyResponses.add(s3Client.copyPart(copyRequest));
            bytePosition += maxChunk;

            LOG.debug("Done " + bytePosition);
        }

        // Complete the upload request to concatenate all uploaded parts and make the copied object available.
        CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                bucket,
                dstKey,
                initResult.getUploadId(),
                getETags(copyResponses));
        s3Client.completeMultipartUpload(completeRequest);

        LOG.debug("Completed multipart copy");
    }

    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<PartETag>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }*/

    /**
     * utility fn to delete unnecessary CSV files left from the Emis SFTP decryption
     * We keep the raw GPG files and the split CSV files, so don't need the interim large decrypted CSV files
     */
    /*private static void deleteCsvs(String bucket, String path, boolean test) {
        LOG.info("Deleting unnecessary CSV files from3 " + bucket + " for " + path);

        AmazonS3ClientBuilder clientBuilder = AmazonS3ClientBuilder
                .standard()
                .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                .withRegion(Regions.EU_WEST_2);


        AmazonS3 s3Client = clientBuilder.build();

        ListObjectsV2Request request = new ListObjectsV2Request();
        request.setBucketName(bucket);
        request.setPrefix(path);

        while (true) {

            ListObjectsV2Result result = s3Client.listObjectsV2(request);
            if (result.getObjectSummaries() != null) {
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String key = objectSummary.getKey();

                    //ignore anything that's not a CSV file
                    String extension = FilenameUtils.getExtension(key);
                    if (!extension.equalsIgnoreCase("csv")) {
                        //LOG.info("Ignoring non-CSV " + key);
                        continue;
                    }

                    //the only CSV files we want to keep are within the "SPLIT" directory hierarchy
                    if (key.toLowerCase().indexOf("split") > -1) {
                        //LOG.info("Ignoring CSV " + key);
                        continue;
                    }

                    if (test) {
                        LOG.info("Would delete " + key);

                    } else {
                        DeleteObjectRequest deleteRequest = new DeleteObjectRequest(bucket, key);
                        s3Client.deleteObject(deleteRequest);

                        LOG.info("Deleted " + key);
                    }
                }
            }

            if (result.isTruncated()) {
                String nextToken = result.getNextContinuationToken();
                request.setContinuationToken(nextToken);
                continue;

            } else {
                break;
            }
        }

        LOG.info("Finished deleting unnecessary CSV files from3 " + bucket + " for " + path);
    }*/

    /*private static void test7zDecompress(String file, String password) {
        try {
            //now we can decompress it
            File src = new File(file);
            SevenZFile sevenZFile = new SevenZFile(src, password.toCharArray());
            SevenZArchiveEntry entry = sevenZFile.getNextEntry();
            //long size = entry.getSize();
            String entryName = entry.getName();

            String unzippedFile = FilenameUtils.concat(src.getParent(), entryName);
            FileOutputStream fos = new FileOutputStream(unzippedFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);


            while (true) {

                //can't get reading in blocks to work, so just do it byte by byte
                int b = sevenZFile.read();
                if (b == -1) {
                    break;
                }
                bos.write(b);
            }

            //close everything
            bos.close();
            sevenZFile.close();

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }*/

    /**
     * one-off routine to populate the new is_bulk column on the batch_split table so we
     * can work out if we've received bulk data for a service or not
     */
    /*private static void loadTPPSRCodetoHahtable(String configurationId, boolean testMode, String odsCodeRegex) throws Exception {
        LOG.info("Checking for Bulks in " + configurationId);
        if (!Strings.isNullOrEmpty(odsCodeRegex)) {
            LOG.info("Restricting to orgs matching " + odsCodeRegex);
        }


        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ConnectionManager.getSftpReaderNonPooledConnection();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            //SftpBulkDetector bulkDetector = ImplementationActivator.createSftpBulkDetector(dbConfiguration);

            DataLayerI dataLayer = configuration.getDataLayer();
            List<Batch> batches = dataLayer.getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            //ensure batches are sorted properly
            batches.sort((o1, o2) -> {
                Integer i1 = o1.getSequenceNumber();
                Integer i2 = o2.getSequenceNumber();
                return i1.compareTo(i2);
            });
            int count = 0;
            for (Batch b: batches) {
                count ++;
                if (count > 1)
                    System.exit(0);
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug(">>>>>>>>Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {


                    String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                    String tempDir = edsConfiguration.getTempDirectory(); //e.g. c:\temp
                    String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
                    String batchRelativePath = b.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00
                    String splitRelativePath = split.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

                    String tempPath = FilenameUtils.concat(tempDir, configurationDir);
                    LOG.trace("permDir: " + permDir + " tempDir: " + tempDir + " configurationDir: " + configurationDir  + " batchRelativePath: "
                            + batchRelativePath  + " splitRelativePath: " + splitRelativePath ) ;
                    tempPath = FilenameUtils.concat(tempPath, splitRelativePath);
                    boolean createdTempPath = new File(tempPath).mkdirs();

                    //LOG.debug(">>>>>>>>>>>>>>>>>>>>>>>Doing batch split " + split.getBatchSplitId());

                        //for TPP, we have to copy Code file to tmp
                        List<String> files = new ArrayList<>();
                        files.add(TppConstants.CODE_FILE);

                        String permPath = FilenameUtils.concat(permDir, configurationDir);
                        LOG.trace("permPath1: " + permPath );
                        permPath = FilenameUtils.concat(permPath, splitRelativePath);
                        LOG.trace("permPath2: " + permPath );


                        List<String> storageContents = FileHelper.listFilesInSharedStorage(permPath);
                        File splitFile = null;
                        for (String filePermPath: storageContents) {
                            String fileName = FilenameUtils.getName(filePermPath);
                            if (files.contains(fileName)) {

                                String fileTempPath = FilenameUtils.concat(tempPath, fileName);

                                try {
                                    InputStream is = FileHelper.readFileFromSharedStorage(filePermPath);
                                    splitFile = new File(fileTempPath);
                                    Files.copy(is, new File(fileTempPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                                    is.close();
                                    //LOG.trace("Copied " + filePermPath + " to " + fileTempPath);
                                } catch (Exception ex) {
                                    LOG.error("Error copying " + filePermPath + " to " + fileTempPath);
                                    throw ex;
                                }
                            }
                        }
                        writeToHashtable(permPath, TppConstants.COL_ROW_IDENTIFIER_TPP, splitFile, batchRelativePath);
                        //delete the tmp directory contents
                        FileHelper.deleteRecursiveIfExists(tempPath);
                }

            }

            LOG.info("Finished checking for Bulks in " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }  finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
*/

}

