package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

public class TppBatchValidator extends SftpBatchValidator {
    private static final Logger LOG = LoggerFactory.getLogger(TppBatchValidator.class);

    public static final String APPOINTMENT_ATTENDEES = "SRAppointmentAttendees";

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

        //ensure our understanding of the SRManifest file is still correct and find the start date for our new extract
        Date incompleteExtractStartDate = checkAllFilesInManifestHaveSameDates(incompleteBatch, manifestRecords);

        //make sure the dates of all records in the manifest file match up with the dates in the previous one
        checkManifestDatesMatch(incompleteBatch, lastCompleteBatch, incompleteExtractStartDate);

        return true;
    }

    /**
     * makes sure that the start and end date for each manifest record is the same and returns the start date
     */
    private Date checkAllFilesInManifestHaveSameDates(Batch incompleteBatch, List<ManifestRecord> manifestRecords) throws SftpValidationException {

        Date dEndFirst = null;
        String endFileFirst = null;
        
        Date dStartFirst = null;
        String startFileFirst = null;

        for (ManifestRecord newRecord : manifestRecords) {
            String file = newRecord.getFileNameWithoutExtension();
            Date dEnd = newRecord.getDateTo();
            Date dStart = newRecord.getDateFrom();

            //there's something consistently wrong with this one file type, with it not having any dates, so just skip it
            if (file.equals(APPOINTMENT_ATTENDEES)) {
                continue;
            }

            //records should always have an end date
            if (dEnd == null) {
                throw new SftpValidationException("No end date in SRManifest for " + file + " in batch " + incompleteBatch.getBatchId());
            }

            if (dEndFirst == null) {
                dEndFirst = dEnd;
                endFileFirst = file;

            } else if (!dEndFirst.equals(dEnd)) {
                //if we find a record with a different end date, then something is wrong
                throw new SftpValidationException("Different end dates in SRManifest for " + file + " (" + dEnd + ") and " + endFileFirst + " (" + dEndFirst + ")" + " in batch " + incompleteBatch.getBatchId());
            }
            
            //manifest records only have a start date if they're deltas
            if (newRecord.isDelta()) {
                
                if (dStart == null) {
                    throw new SftpValidationException("No start date in SRManifest for " + file + " in batch " + incompleteBatch.getBatchId());
                }

                if (dStartFirst == null) {
                    dStartFirst = dStart;
                    startFileFirst = file;

                } else if (!dStartFirst.equals(dStart)) {
                    //if we find a record with a different start date, then something is wrong
                    throw new SftpValidationException("Different start dates in SRManifest for " + file + " (" + dStart + ") and " + startFileFirst + " (" + dStartFirst + ")" + " in batch " + incompleteBatch.getBatchId());
                }
            }
        }

        return dStartFirst;
    }

    private void checkManifestDatesMatch(Batch incompleteBatch, Batch lastCompleteBatch, Date incompleteExtractStartDate) throws SftpValidationException {

        //if we're the first batch, then do nothing
        if (lastCompleteBatch == null) {
            return;
        }

        //if we don't have a start date for our extract, we're a bulk or re-bulk so don't need to validate that deltas join up
        if (incompleteExtractStartDate == null) {
            return;
        }

        Date lastExtractEndDate = lastCompleteBatch.getExtractCutoff();
        if (lastExtractEndDate == null) {
            //the last extract date has been set on all "last" batches as of now, so
            //this exception is safe to go in.
            throw new SftpValidationException("End date of last batch is null, batch ID " + incompleteBatch.getBatchId());
            /*LOG.warn("Previous batch " + lastCompleteBatch.getBatchId() + " has null extract cutoff, so cannot check new batch " + incompleteBatch.getBatchId());
            return;*/
        }

        //if the start date of the new extract doesn't match the end date of the last one, then something is wrong
        //SD-353 - when TPP re-bulk files, we end up with the start of the new extract being BEFORE the end of the previous delta. No idea
        //why this is the case, but the data looks OK, so only validate if we have a gap in data.
        //if (!lastExtractEndDate.equals(incompleteExtractStartDate)) {
        if (incompleteExtractStartDate.after(lastExtractEndDate)) {
            throw new SftpValidationException("Start date of new batch (" + incompleteExtractStartDate + ") does not match end date of last batch (" + lastExtractEndDate + ") in new batch " + incompleteBatch.getBatchId());
        }
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
        String manifestFilePath = FilenameUtils.concat(dir, TppConstants.MANIFEST_FILE);
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
            if (fileName.equalsIgnoreCase(TppConstants.MANIFEST_FILE)) {
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