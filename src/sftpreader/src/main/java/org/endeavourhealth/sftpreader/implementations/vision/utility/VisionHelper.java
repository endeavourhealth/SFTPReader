package org.endeavourhealth.sftpreader.implementations.vision.utility;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.util.List;

public class VisionHelper {

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    public static final String PATIENT_FILE_TYPE = "patient_data_extract";
    public static final String JOURNAL_FILE_TYPE = "journal_data_extract";

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
