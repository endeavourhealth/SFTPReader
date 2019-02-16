package org.endeavourhealth.sftpreader.implementations.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EmisHelper {

    public static final String EMIS_AGREEMENTS_FILE_ID = "Agreements_SharingOrganisation";
    public static final String EMIS_ORGANISATION_FILE_ID = "Admin_Organisation";

    public static String findSharingAgreementsFileInTempDir(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws SftpValidationException {
        return findFileInTempDir(instanceConfiguration, dbConfiguration, batch, EMIS_AGREEMENTS_FILE_ID);
    }

    public static String findOrganisationFileInTempDir(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws SftpValidationException {
        return findFileInTempDir(instanceConfiguration, dbConfiguration, batch, EMIS_ORGANISATION_FILE_ID);
    }

    private static String findFileInTempDir(DbInstanceEds instanceConfiguration,
                                  DbConfiguration dbConfiguration,
                                  Batch batch,
                                  String fileIdentifier) throws SftpValidationException {

        String fileName = null;

        for (BatchFile batchFile: batch.getBatchFiles()) {
            if (batchFile.getFileTypeIdentifier().equalsIgnoreCase(fileIdentifier)) {
                fileName = EmisFilenameParser.getDecryptedFileName(batchFile, dbConfiguration);
            }
        }

        if (Strings.isNullOrEmpty(fileName)) {
            throw new SftpValidationException("Failed to find file for identifier " + fileIdentifier + " in batch " + batch.getBatchId());
        }

        String tempStoragePath = instanceConfiguration.getTempDirectory();
        //String sharedStoragePath = instanceConfiguration.getSharedStoragePath();
        String configurationPath = dbConfiguration.getLocalRootPath();
        String batchPath = batch.getLocalRelativePath();

        String tempPath = FilenameUtils.concat(tempStoragePath, configurationPath);
        tempPath = FilenameUtils.concat(tempPath, batchPath);
        tempPath = FilenameUtils.concat(tempPath, fileName);

        //in most cases, we should be able to find the file in our temporary storage,
        //even through it will also have been written to permanent storage.
        File tempFile = new File(tempPath);
        if (tempFile.exists()) {
            return tempPath;
        }

        //we don't keep the un-split files in permanent storage now, so the below won't work
        throw new SftpValidationException("Failed to find " + fileIdentifier + " in temp dir");
        //if it doesn't exist in temp, then we need to return the permanent path
        /*String permanentPath = FilenameUtils.concat(sharedStoragePath, configurationPath);
        permanentPath = FilenameUtils.concat(permanentPath, batchPath);
        permanentPath = FilenameUtils.concat(permanentPath, fileName);

        return permanentPath;*/
    }

    public static Map<String, SharingAgreementRecord> readSharingAgreementsFile(String filePath) throws SftpValidationException {

        Map<String, SharingAgreementRecord> ret = new HashMap<>();

        CSVParser csvParser = null;
        try {
            InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(filePath);
            csvParser = new CSVParser(isr, EmisBatchSplitter.CSV_FORMAT.withHeader());

            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                String orgGuid = csvRecord.get("OrganisationGuid");
                String activated = csvRecord.get("IsActivated");
                String disabled = csvRecord.get("Disabled");
                String deleted = csvRecord.get("Deleted");

                SharingAgreementRecord r = new SharingAgreementRecord(orgGuid, activated.equalsIgnoreCase("true"), disabled.equalsIgnoreCase("true"), deleted.equalsIgnoreCase("true"));
                ret.put(orgGuid, r);
            }
        } catch (Exception ex) {
            throw new SftpValidationException("Failed to read sharing agreements file " + filePath, ex);

        } finally {
            try {
                if (csvParser != null) {
                    csvParser.close();
                }
            } catch (IOException ioe) {
                //if we fail to close, then it doesn't matter
            }
        }

        return ret;
    }

    public static String findSharingAgreementsFileInPermanentDir(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch, String odsCode) throws Exception {
        return findFileInPermanentDir(db, instanceConfiguration, dbConfiguration, batch, odsCode, EMIS_AGREEMENTS_FILE_ID);
    }

    /**
     * once in permanent storage, we only ever keep post-split files, so can only get a file in the context of an org
     */
    private static String findFileInPermanentDir(DataLayerI db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration,
                                            Batch batch, String odsCode,
                                            String fileIdentifier) throws Exception {

        //find the file name we're after
        String fileName = null;
        for (BatchFile batchFile: batch.getBatchFiles()) {
            String batchFileType = batchFile.getFileTypeIdentifier();

            if (batchFileType.equalsIgnoreCase(fileIdentifier)) {
                fileName = EmisFilenameParser.getDecryptedFileName(batchFile, dbConfiguration);
                break;
            }
        }
        if (Strings.isNullOrEmpty(fileName)) {
            throw new SftpValidationException("Failed to find file for identifier " + fileIdentifier + " in batch " + batch.getBatchId());
        }

        //find the batch split for the org
        BatchSplit batchSplit = null;
        List<BatchSplit> batchSplits = db.getBatchSplitsForBatch(batch.getBatchId());
        for (BatchSplit s: batchSplits) {
            if (s.getOrganisationId().equalsIgnoreCase(odsCode)) {
                batchSplit = s;
                break;
            }
        }
        if (batchSplit == null) {
            throw new SftpValidationException("Failed to find batch split for ODS code " + odsCode + " in batch " + batch.getBatchId());
        }

        String sharedStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://discoverysftplanding/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS001
        String splitPath = batchSplit.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}

        String path = FilenameUtils.concat(sharedStoragePath, configurationPath);
        path = FilenameUtils.concat(path, splitPath);
        path = FilenameUtils.concat(path, fileName);

        return path;
    }
}
