package org.endeavourhealth.sftpreader.implementations.bhrut.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;

public class BhrutHelper {

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader(
            "LineStatus",
            "PATIENT_EXTERNAL_ID",
            "PAS_ID",
            "FORENAME",
            "SURNAME",
            "NHS_NUMBER",
            "GENDER_CODE",
            "BIRTH_DTTM",
            "DEATH_DTTM",
            "ADDRESS1",
            "ADDRESS2",
            "ADDRESS3",
            "ADDRESS4",
            "ADDRESS5",
            "POSTCODE",
            "SENSITIVE_PDS_FLAG",
            "SENSITIVE_LOCAL_FLAG",
            "HPHONE_NUMBER",
            "HPHONE_NUMBER_CONSENT",
            "MPHONE_NUMBER",
            "MPHONE_NUMBER_CONSENT",
            "ETHNICITY_CODE",
            "FOREIGN_LANGUAGE_CODE",
            "NOK_NAME",
            "NOKREL_NHSCODE");

    public static final String PATIENT_PMI_FILE_TYPE = "PMI";

    /**
     * finds a Bhrut data file in the temporary directory (note that Bhrut files don't get split
     * so this is simpler than the equivalent function for Emis)
     */
    public static String findFileInTempDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    Batch batch,
                                                    String fileIdentifier) throws SftpValidationException {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/BHRUT
        String batchPath = batch.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35

        String tempPath = FilenameUtils.concat(tempStoragePath, configurationPath);
        tempPath = FilenameUtils.concat(tempPath, batchPath);

        File tempDir = new File(tempPath);
        if (!tempDir.exists()) {
            throw new SftpValidationException("Temp directory " + tempDir + " doesn't exist");
        }

        String filePath = null;

        for (File f: tempDir.listFiles()) {
            String name = f.getName();
            String ext = FilenameUtils.getExtension(name);
            if (ext.equalsIgnoreCase("csv")) {

                RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Vision filename parsing
                VisionFilenameParser parser = new VisionFilenameParser(false, r, dbConfiguration);
                String fileType = parser.generateFileTypeIdentifier();
                if (fileType.equals(fileIdentifier)) {
                    filePath = f.getAbsolutePath();
                    break;
                }
            }
        }

        if (filePath == null) {
            //Bhrut extracts don't always contain all file types, so just return null
            //throw new SftpValidationException("Failed to find " + fileIdentifier + " file in " + tempDir);
            return null;
        }

        return filePath;
    }
}
