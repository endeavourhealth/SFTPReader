package org.endeavourhealth.sftpreader;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.google.common.base.Strings;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.postgres.PgAppLock.PgAppLock;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.barts.BartsSftpFilenameParser;
import org.endeavourhealth.sftpreader.management.ManagementService;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;


public class Main {

	public static final String PROGRAM_DISPLAY_NAME = "SFTP Reader";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static SftpReaderTaskScheduler sftpReaderTaskScheduler;
    private static ManagementService managementService;
    //private static SlackNotifier slackNotifier;

	public static void main(String[] args) {
		try {
            configuration = Configuration.getInstance();

            if (args.length > 0) {
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


                        /*for (String okLine: ok) {
                            LOG.info(okLine);
                        }*/
                    }
                }
            }



            if (args.length > 0) {
                if (args[0].equalsIgnoreCase("FixS3")) {
                    String bucket = args[1];
                    String path = args[2];
                    boolean test = Boolean.parseBoolean(args[3]);
                    fixS3(bucket, path, test);
                    System.exit(0);
                }
            }

            if (args.length > 0) {
                if (args[0].equalsIgnoreCase("DeleteCsvs")) {
                    String bucket = args[1];
                    String path = args[2];
                    boolean test = Boolean.parseBoolean(args[3]);
                    deleteCsvs(bucket, path, test);
                    System.exit(0);
                }
            }

            PgAppLock pgAppLock = new PgAppLock(configuration.getInstanceName(), configuration.getNonPooledDatabaseConnection());
            try {

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

            } finally {
                pgAppLock.releaseLock();
            }
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

    private static void fixS3(String bucket, String path, boolean test) {
        LOG.info("Fixing S3 " + bucket + " for " + path);

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
                    LOG.info("" + key + " has encryption " + encryption);

                    if (!test
                        && Strings.isNullOrEmpty(encryption)) {

                        String key2 = key + "_COPY";

                        //copy to backup
                        CopyObjectRequest copyRequest = new CopyObjectRequest(bucket, key, bucket, key2);
                        s3Client.copyObject(copyRequest);

                        ObjectMetadata objectMetadata = new ObjectMetadata();
                        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);

                        //copy back to original but WITH encryption
                        copyRequest = new CopyObjectRequest(bucket, key2, bucket, key);
                        copyRequest.setNewObjectMetadata(objectMetadata);
                        s3Client.copyObject(copyRequest);

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
    }

    /**
     * utility fn to delete unnecessary CSV files left from the Emis SFTP decryption
     * We keep the raw GPG files and the split CSV files, so don't need the interim large decrypted CSV files
     */
    private static void deleteCsvs(String bucket, String path, boolean test) {
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
    }
}

