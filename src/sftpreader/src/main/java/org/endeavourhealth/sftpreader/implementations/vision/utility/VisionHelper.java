package org.endeavourhealth.sftpreader.implementations.vision.utility;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraConstants;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.util.List;

public class VisionHelper {

    public static CSVFormat getCsvFormat(String fileTypeId) {
        String[] cols = getCsvHeaders(fileTypeId);
        return VisionConstants.CSV_FORMAT.withHeader(cols);
    }

    private static String[] getCsvHeaders(String fileTypeId) {

        if (fileTypeId.equals(VisionConstants.FILE_ID_PATIENT)) {
            return new String[]{
                    "PID",
                    "REFERENCE",
                    "DATE_OF_BIRTH",
                    "SEX",
                    "POSTCODE",
                    "MARITAL_STATUS",
                    "GP",
                    "GP_USUAL",
                    "ACTIVE",
                    "REGISTERED_DATE",
                    "REMOVED_DATE",
                    "HA",
                    "PCG",
                    "SURGERY",
                    "MILEAGE",
                    "DISPENSING",
                    "ETHNIC",
                    "DATE_OF_DEATH",
                    "PRACTICE",
                    "SURNAME",
                    "FORENAME",
                    "TITLE",
                    "NHS_NUMBER",
                    "ADDRESS",
                    "ADDRESS_1",
                    "ADDRESS_2",
                    "ADDRESS_3",
                    "ADDRESS_4",
                    "ADDRESS_5",
                    "PHONE_NUMBER",
                    "MOBILE_NUMBER",
                    "EMAIL",
                    "PRACT_NUMBER",
                    "SERVICE_ID",
                    "ACTION"
            };

        } else if (fileTypeId.equals(VisionConstants.FILE_ID_JOURNAL)) {
            return new String[]{
                    "PID",
                    "ID",
                    "DATE",
                    "RECORDED_DATE",
                    "CODE",
                    "SNOMED_CODE",
                    "BNF_CODE",
                    "HCP",
                    "HCP_TYPE",
                    "GMS",
                    "EPISODE",
                    "TEXT",
                    "RUBRIC",
                    "DRUG_FORM",
                    "DRUG_STRENGTH",
                    "DRUG_PACKSIZE",
                    "DMD_CODE",
                    "IMMS_STATUS",
                    "IMMS_COMPOUND",
                    "IMMS_SOURCE",
                    "IMMS_BATCH",
                    "IMMS_REASON",
                    "IMMS_METHOD",
                    "IMMS_SITE",
                    "ENTITY",
                    "VALUE1_NAME",
                    "VALUE1",
                    "VALUE1_UNITS",
                    "VALUE2_NAME",
                    "VALUE2",
                    "VALUE2_UNITS",
                    "END_DATE",
                    "TIME",
                    "CONTEXT",
                    "CERTAINTY",
                    "SEVERITY",
                    "LINKS",
                    "LINKS_EXT",
                    "SERVICE_ID",
                    "ACTION",
                    "SUBSET",
                    "DOCUMENT_ID"
            };

        } else {
            //there are a bunch of other Vision files, but I've only given the columns for the ones we need to know
            throw new RuntimeException("Unexpected file type " + fileTypeId);
        }
    }

    /**
     * finds a Vision data file in the temporary directory (note that Vision files don't get split
     * so this is simpler than the equivalent function for Emis)
     */
    public static String findFileInTempDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    Batch batch,
                                                    String fileIdentifier) throws SftpValidationException {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/VISION
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
            //Vision extracts don't always contain all file types, so just return null
            //throw new SftpValidationException("Failed to find " + fileIdentifier + " file in " + tempDir);
            return null;
        }

        return filePath;
    }


}
