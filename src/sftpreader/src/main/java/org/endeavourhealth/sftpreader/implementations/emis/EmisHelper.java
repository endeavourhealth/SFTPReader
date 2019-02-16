package org.endeavourhealth.sftpreader.implementations.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class EmisHelper {

    public static final String EMIS_AGREEMENTS_FILE_ID = "Agreements_SharingOrganisation";
    public static final String EMIS_ORGANISATION_FILE_ID = "Admin_Organisation";

    public static String findSharingAgreementsFile(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws SftpValidationException {
        return findFile(instanceConfiguration, dbConfiguration, batch, EMIS_AGREEMENTS_FILE_ID);
    }

    public static String findOrganisationFile(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws SftpValidationException {
        return findFile(instanceConfiguration, dbConfiguration, batch, EMIS_ORGANISATION_FILE_ID);
    }

    public static String findFile(DbInstanceEds instanceConfiguration,
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
        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();
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

        //if it doesn't exist in temp, then we need to return the permanent path
        String permanentPath = FilenameUtils.concat(sharedStoragePath, configurationPath);
        permanentPath = FilenameUtils.concat(permanentPath, batchPath);
        permanentPath = FilenameUtils.concat(permanentPath, fileName);

        return permanentPath;
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
}
