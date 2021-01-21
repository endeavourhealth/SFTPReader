package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.*;

public class TppBatchValidator extends SftpBatchValidator {

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);
    private static final String MANIFEST_FILE = "SRManifest.csv";
    private static final String REQUIRED_CHARSET = "Cp1252";

    public static final String APPOINTMENT_ATTENDEES = "SRAppointmentAttendees";

    private static final Logger LOG = LoggerFactory.getLogger(TppBatchValidator.class);

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        int batchFileCount = incompleteBatch.getBatchFiles().size();
        if (batchFileCount == 0) {
            throw new SftpValidationException("Incorrect number of files 0 in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

        //check for presence of the SRManifest.csv to signify that it is a complete batch
        validateManifestFileFound(incompleteBatch);

        //for the next checks, we need to process the content of the manifest file
        String tempDir = getTempDirectoryPathForBatch(instanceConfiguration, dbConfiguration, incompleteBatch);
        List<ManifestRecord> manifestRecords = readManifestFromDirectory(tempDir);

        //make sure that all files listed in the manifest file are present
        checkAllFilesInManifestArePresent(instanceConfiguration, dbConfiguration, incompleteBatch, manifestRecords);

        //make sure the dates of all records in the manifest file match up with the dates in the previous one
        //checkManfiestDatesMatch(incompleteBatch, lastCompleteBatch, db, manifestRecords);

        return true;
    }

    private void checkManfiestDatesMatch(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                         Batch incompleteBatch, Batch lastCompleteBatch, DataLayerI db,
                                         List<ManifestRecord> newRecords) throws SftpValidationException {

        //if we're the first batch, then do nothing
        if (lastCompleteBatch == null) {
            return;
        }
/*
        //we need to get the manifest records from the previous batch. We don't store the files at the batch level in the permanent storage,
        //so need to get it from one (any) of the "split" sub-directories
        int lastBatchId = lastCompleteBatch.getBatchId();
        List<BatchSplit> splits = db.getBatchSplitsForBatch(lastBatchId);
        if (splits.isEmpty()) {
            throw new SftpValidationException("Unexpected zero splits in last complete batch " + lastBatchId);
        }
        BatchSplit split = splits.get(0); //doesn't matter which we use, since they all have an identical copy

        String splitDir = getPersonDirectoryPathForSplit(instanceConfiguration, dbConfiguration, split);
        List<ManifestRecord> lastRecords = readManifestFromDirectory(splitDir);

        //hash the previous one by file name
        Map<String, ManifestRecord> hmLast = new HashMap<>();

        for (ManifestRecord lastRecord: lastRecords) {
            String file = lastRecord.getFileNameWithoutExtension();
            hmLast.put(file, lastRecord);
        }

        //now check everything in the new file
        for (ManifestRecord newRecord: newRecords) {
            String file = newRecord.getFileNameWithoutExtension();

            //there's some kind of bug in SystmOne where the manifest record for this one file seems to be
            //without either start or end date most of the time. Since we don't actually process this file, I think
            //it reasonable to just skip this file for this validation, since we are still validating 100+ other files.
            if (file.equals(APPOINTMENT_ATTENDEES)) {
                continue;
            }
            
            ManifestRecord lastRecord = hmLast.get(file);

            if (lastRecord == null) {
                //TODO - this means we have a new file, so that's interesting

            } else {

                Date newStart = newRecord.getDateFrom();
                Date lastEnd = lastRecord.getDateTo();

                //TODO - handle start date being null (i.e. is a bulk?)
                //TODO - handle end date being null ???

                //TODO - handle "SRAppointmentAttendees" having always null start and end

                if (newStart.equals(lastEnd)) {
                    //ideally the start date of the new record should match the end date of the last one

                } else if (newS)

            }
        }*/
    }

    /**
     * works out the directory path for the batch's directory in the PERMANENT storage drive
     */
    private String getPersonDirectoryPathForSplit(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, BatchSplit split) {

        String sharedStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS001
        String splitPath = split.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}

        return FileHelper.concatFilePath(sharedStoragePath, configurationPath, splitPath);
    }

    /**
     * works out the directory path for the batch's directory in the TEMP drive
     */
    public static String getTempDirectoryPathForBatch(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) {
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        return FileHelper.concatFilePath(tempDir, configurationDir, batchDir);
    }

    public static List<ManifestRecord> readManifestFromDirectory(String dir) throws SftpValidationException {

        //read the manifest file and compare against the contents of the batch. Using the temp directory
        //as all files, including blank ones, will exist in temp storage
        String manifestFilePath = FilenameUtils.concat(dir, MANIFEST_FILE);
        File manifestFile = new File(manifestFilePath);

        try {
            List<ManifestRecord> records = ManifestRecord.readManifestFile(manifestFile);
            return records;
        } catch (Exception ex) {
            throw new SftpValidationException("Failed to read manifest file " + manifestFile, ex);
        }
    }

    private void validateManifestFileFound(Batch incompleteBatch) throws SftpValidationException {
        List<BatchFile> batchFiles = incompleteBatch.getBatchFiles();
        boolean foundManifest = false;
        File manifestFile = null;
        for (BatchFile file: batchFiles) {

            String fileName = file.getFilename();
            if (fileName.equalsIgnoreCase(MANIFEST_FILE)) {
                foundManifest = true;
                break;
            }
        }

        //no manifest file means an incomplete batch, throw an exception so subsequent batches do not process
        if (!foundManifest) {
            throw new SftpValidationException("SRManifest.csv file missing from batch " + incompleteBatch.getBatchId());
        }
    }

    /**
     * validates that every file listed in SRManifest is present
     */
    private void checkAllFilesInManifestArePresent(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                                   Batch incompleteBatch, List<ManifestRecord> manifestRecords) throws SftpValidationException {

        String tempDir = getTempDirectoryPathForBatch(instanceConfiguration, dbConfiguration, incompleteBatch);
        List<String> missingFiles = new ArrayList<>();

        for (ManifestRecord record: manifestRecords) {

            String fileName = record.getFileNameWithExtension();
            String filePath = FilenameUtils.concat(tempDir, fileName);
            File f = new File(filePath);
            if (!f.exists()) {
                missingFiles.add(fileName);
            }
        }

        if (!missingFiles.isEmpty()) {
            throw new SftpValidationException("SRManifest.csv file(s) [" + missingFiles + "] missing from temp folder for batch " + incompleteBatch.getBatchId());
        }
    }
}