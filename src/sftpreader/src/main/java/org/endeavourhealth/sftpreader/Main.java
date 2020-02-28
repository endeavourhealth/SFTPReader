package org.endeavourhealth.sftpreader;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Strings;
import org.apache.commons.csv.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.common.utility.MetricsHelper;
import org.endeavourhealth.core.application.ApplicationHeartbeatHelper;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisFixDisabledService;
import org.endeavourhealth.sftpreader.implementations.tpp.TppBulkDetector;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.vision.VisionBulkDetector;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvJoiner;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.endeavourhealth.sftpreader.utilities.PgpUtil;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.*;


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



            if (args.length > 0) {
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

                if (args[0].equalsIgnoreCase("DecryptGpg")) {
                    String filePath = args[1];
                    String configurationId = args[2];
                    decryptGpgFile(filePath, configurationId);
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

                if (args[1].equalsIgnoreCase("CheckForBulks")) {
                    String configurationId = args[2];
                    checkForBulks(configurationId);
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
            sftpReaderTaskScheduler.start();

            //now we're running, start this
            MetricsHelper.startHeartbeat();
            ApplicationHeartbeatHelper.start(sftpReaderTaskScheduler);

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

    private static void checkForBulks(String configurationId) throws Exception {
        LOG.info("Checking for Bulks in " + configurationId);

        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ConnectionManager.getSftpReaderConnection();

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

            for (Batch b: batches) {
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug("Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {
                    if (split.isBulk()) {
                        continue;
                    }

                    String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                    String tempDir = edsConfiguration.getTempDirectory(); //e.g. c:\temp
                    String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
                    String batchRelativePath = b.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00
                    String splitRelativePath = split.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

                    String tempPath = FilenameUtils.concat(tempDir, configurationDir);
                    tempPath = FilenameUtils.concat(tempPath, splitRelativePath);
                    new File(tempPath).mkdirs();


                    if (bulkDetector instanceof TppBulkDetector) {
                        //for TPP, we have to copy the Manifest file to tmp
                        String permPath = FilenameUtils.concat(permDir, configurationDir);
                        permPath = FilenameUtils.concat(permPath, batchRelativePath); //don't have this in the split path
                        String perManifestPath = FilenameUtils.concat(permPath, TppConstants.MANIFEST_FILE);

                        String tempManifestPath = FilenameUtils.concat(tempPath, TppConstants.MANIFEST_FILE);

                        InputStream is = FileHelper.readFileFromSharedStorage(perManifestPath);
                        Files.copy(is, new File(tempManifestPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        is.close();

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
                            if (fileType.equals(VisionHelper.PATIENT_FILE_TYPE)) {
                                permPatientPath = file;
                            } else if (fileType.equals(VisionHelper.JOURNAL_FILE_TYPE)) {
                                permJournalPath = file;
                            }
                        }

                        String patientFileName = FilenameUtils.getName(permPatientPath);
                        String journalFileName = FilenameUtils.getName(permJournalPath);

                        String tmpPatientPath = FilenameUtils.concat(tempPath, patientFileName);
                        String tmpJournalPath = FilenameUtils.concat(tempPath, journalFileName);

                        InputStream is = FileHelper.readFileFromSharedStorage(permPatientPath);
                        Files.copy(is, new File(tmpPatientPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        is.close();

                        is = FileHelper.readFileFromSharedStorage(permJournalPath);
                        Files.copy(is, new File(tmpJournalPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                        is.close();

                    } else {
                        throw new Exception("Not implemented this yet for " + bulkDetector.getClass().getSimpleName());
                    }

                    boolean isBulk = bulkDetector.isBulkExtract(b, split, dataLayer, edsConfiguration, dbConfiguration);
                    if (isBulk) {
                        LOG.debug("    Found bulk for " + split.getOrganisationId());
                    }

                    ps.setBoolean(1, isBulk);
                    ps.setInt(2, split.getBatchSplitId());

                    ps.executeUpdate();
                    conn.commit();

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

    private static void testSplitting(String srcFilePath) {
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
    }

    private static void decryptGpgFile(String filePath, String configurationId) {
        LOG.info("Decrypting " + filePath + " from configuration " + configurationId);
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

            InputStream inputStream = FileHelper.readFileFromSharedStorage(filePath);

            String privateKey = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKey();
            String privateKeyPassword = dbConfiguration.getPgpConfiguration().getPgpRecipientPrivateKeyPassword();
            String publicKey = dbConfiguration.getPgpConfiguration().getPgpSenderPublicKey();

            String ext = FilenameUtils.getExtension(filePath);
            int len = ext.length() + 1;
            String decryptedTempFile = filePath.substring(0, filePath.length()-len);

            try {
                LOG.info("   Decrypting file to: " + decryptedTempFile);
                PgpUtil.decryptAndVerify(inputStream, decryptedTempFile, privateKey, privateKeyPassword, publicKey);

            } finally {
                inputStream.close();
            }

            LOG.info("Finished Decrypting " + filePath + " from configuration " + configurationId);
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
}

