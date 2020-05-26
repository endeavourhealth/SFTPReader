package org.endeavourhealth.sftpreader.implementations.tpp.utility;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;

public class TppHelper {

    /**
     * finds the file of the given "type" in the temp directory structure
     * Note that although this looks similar to the equivalent Emis and Vision functions, it is different since
     * all three suppliers send data differently
     */
    public static String findPostSplitFileInTempDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    Batch batch,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws SftpValidationException {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/TPP/YDDH3_08W
        String batchSplitPath = batchSplit.getLocalRelativePath(); //e.g. 2019-12-09T00.15.00/Split/E87065/

        String tempPath = FilenameUtils.concat(tempStoragePath, configurationPath);
        tempPath = FilenameUtils.concat(tempPath, batchSplitPath);

        File tempDir = new File(tempPath);
        if (!tempDir.exists()) {
            throw new SftpValidationException("Temp directory " + tempDir + " doesn't exist");
        }

        String filePath = null;

        for (File f: tempDir.listFiles()) {
            String name = f.getName();
            String ext = FilenameUtils.getExtension(name);
            if (ext.equalsIgnoreCase("csv")) {

                RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for TPP filename parsing
                TppFilenameParser parser = new TppFilenameParser(false, r, dbConfiguration);
                String fileType = parser.generateFileTypeIdentifier();
                if (fileType.equals(fileIdentifier)) {
                    filePath = f.getAbsolutePath();
                    break;
                }
            }
        }

        if (filePath == null) {
            //TPP extracts don't always contain all file types, so just return null
            return null;
        }

        return filePath;
    }
}
