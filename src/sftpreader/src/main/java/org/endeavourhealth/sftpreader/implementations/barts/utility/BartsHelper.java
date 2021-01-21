package org.endeavourhealth.sftpreader.implementations.barts.utility;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.barts.BartsFilenameParser;
import org.endeavourhealth.sftpreader.implementations.bhrut.BhrutFilenameParser;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BartsHelper {


    /**
     * finds multiple data file in the temporary directory
     *
     * this is similar to other functions for other publishers, but Barts sends multiples
     * of some file types, and has a wide range of file extensions
     */
    public static List<String> findFilesInTempDir(DbInstanceEds instanceConfiguration,
                                                  DbConfiguration dbConfiguration,
                                                  Batch batch,
                                                  String fileIdentifier) throws SftpValidationException {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/BARTSDW
        String batchPath = batch.getLocalRelativePath(); //e.g. 2019-02-13

        String tempPath = FilenameUtils.concat(tempStoragePath, configurationPath);
        tempPath = FilenameUtils.concat(tempPath, batchPath);

        File tempDir = new File(tempPath);
        if (!tempDir.exists()) {
            throw new SftpValidationException("Temp directory " + tempDir + " doesn't exist");
        }

        List<String> ret = new ArrayList<>();

        for (File f: tempDir.listFiles()) {
            String name = f.getName();

            RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Vision filename parsing
            BartsFilenameParser parser = new BartsFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                String filePath = f.getAbsolutePath();
                ret.add(filePath);
            }
        }

        return ret;
    }
}
