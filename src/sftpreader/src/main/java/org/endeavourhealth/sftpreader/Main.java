package org.endeavourhealth.sftpreader;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.csv.*;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.barts.BartsFilenameParser;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFixDisabledService;
import org.endeavourhealth.sftpreader.implementations.emisCustom.EmisCustomFilenameParser;
import org.endeavourhealth.sftpreader.management.ManagementService;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstance;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.db.EmisOrganisationMap;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.CsvJoiner;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.swing.*;
import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;


public class Main {

	public static final String PROGRAM_DISPLAY_NAME = "SFTP Reader";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static SftpReaderTaskScheduler sftpReaderTaskScheduler;
    private static ManagementService managementService;
    //private static SlackNotifier slackNotifier;

	public static void main(String[] args) {
		try {

            String instanceName = System.getProperty(Configuration.INSTANCE_NAME_JAVA_PROPERTY);
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

            /*if (args.length > 0) {
                if (args[0].equalsIgnoreCase("TestBarts")) {

                    File f = new File("C:\\Users\\drewl\\Desktop\\RAW_barts_ftp_files.txt");
                    List<String> lines = Files.readAllLines(f.toPath());
                    DbConfiguration dbConfiguration = configuration.getConfiguration("BARTSDW");

                    List<String> ok = new ArrayList<>();

                    for (String line : lines) {

                        String dateTok = line.substring(35, 47);
                        if (dateTok.startsWith("DEC")) {
                            dateTok = "2017 " + dateTok;
                        } else {
                            dateTok = "2018 " + dateTok;
                        }
                        dateTok = dateTok.replaceAll("  ", " 0");
                        LocalDateTime extractDate = LocalDateTime.parse(dateTok, DateTimeFormatter.ofPattern("yyyy MMM dd HH:mm"));

                        String filename = line.substring(48);

                        if (filename.equalsIgnoreCase(".")
                                || filename.equalsIgnoreCase("..")) {
                            continue;
                        }

                        BartsSftpFilenameParser parser = new BartsSftpFilenameParser(filename, extractDate, dbConfiguration);
                        if (parser.isFilenameValid()) {
                            ok.add("" + line + " -> " + parser.isFilenameValid() + ", " + parser.generateFileTypeIdentifier() + ", " + parser.generateBatchIdentifier());
                        } else {
                            LOG.error(filename);
                        }


                        *//*for (String okLine: ok) {
                            LOG.info(okLine);
                        }*//*
                    }
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

                if (args[0].equalsIgnoreCase("CheckS3")) {
                    String bucket = args[1];
                    String path = args[2];
                    checkS3(bucket, path);
                    System.exit(0);
                }

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

                if (args[0].equalsIgnoreCase("TestOriginalTerms")) {
                    testOriginalTerms();
                    System.exit(0);
                }

                if (args[0].equalsIgnoreCase("TestDisabledFix")) {
                    String odsCode = args[1];
                    String configurationId = args[2];
                    testDisabledFix(odsCode, configurationId);
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
            LOG.info(PROGRAM_DISPLAY_NAME);
            LOG.info("--------------------------------------------------");

            LOG.info("Instance " + configuration.getInstanceName() + " on host " + configuration.getMachineName());
            LOG.info("Processing configuration(s): " + configuration.getConfigurationIdsForDisplay());

            /*slackNotifier = new SlackNotifier(configuration);
            slackNotifier.notifyStartup();*/

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

            managementService = new ManagementService(configuration);
            managementService.start();

            sftpReaderTaskScheduler = new SftpReaderTaskScheduler(configuration);
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

    private static void testOriginalTerms() {

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

            /*File srcFile = f.getSelectedFile();
            LOG.debug("Selected " + srcFile + " len " + srcFile.length());

            //now we can decompress it
            SevenZFile sevenZFile = new SevenZFile(srcFile, "Emis123".toCharArray());
            SevenZArchiveEntry entry = sevenZFile.getNextEntry();
            //long size = entry.getSize();
            String entryName = entry.getName();

            String tempDir = srcFile.getParent();

            String unzippedFile = FilenameUtils.concat(tempDir, entryName);
            FileHelper.deleteRecursiveIfExists(unzippedFile);
            FileOutputStream fos = new FileOutputStream(unzippedFile);
            BufferedOutputStream bos = new BufferedOutputStream(fos);

            //the file Emis provide doesn't contain column headings, but later things are easier if we have them, so just insert them first
            String headers = "OrganisationCdb\tOrganisationOds\tPatientGuid\tObservationGuid\tOriginalTerm\r\n";

            bos.write(headers.getBytes());
            bos.flush();

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

            LOG.debug("Finished unzip");*/


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
            CsvSplitter csvSplitter = new CsvSplitter(fixedSrcFile, dstDir, csvFormat, "OrganisationOds");
            //CsvSplitter csvSplitter = new CsvSplitter(unzippedFile, dstDir, csvFormat, "OrganisationOds");
            List<File> splitFiles = csvSplitter.go();
            LOG.debug("Finished split into " + splitFiles.size());
            for (File splitFile: splitFiles) {
                LOG.debug("    " + splitFile);
            }

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }

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

            /*if (slackNotifier != null)
                slackNotifier.notifyShutdown();*/

            if (managementService != null)
                managementService.stop();

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

    private static void checkS3(String bucket, String path) {
        LOG.info("Checking S3 " + bucket + " for " + path);

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

        LOG.info("Finished Checking S3 " + bucket + " for " + path);
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

