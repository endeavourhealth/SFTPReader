package org.endeavourhealth.sftpreader.implementations.bhrut.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.adastra.AdastraFilenameParser;
import org.endeavourhealth.sftpreader.implementations.bhrut.BhrutFilenameParser;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.util.List;

public class BhrutHelper {


    /**
     * finds a Bhrut data file in the permanent storage
     */
    public static String findFileInPermDir(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   Batch batch,
                                                   String fileIdentifier) throws Exception {

        String tempStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/ADASTRA
        String batchPath = batch.getLocalRelativePath(); //e.g. 2021-01-21T00.00.02

        String dirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchPath);

        List<String> files = FileHelper.listFilesInSharedStorage(dirPath);

        if (files == null || files.isEmpty()) {
            throw new SftpValidationException("Failed to find any files in " + dirPath);
        }

        for (String filePath: files) {
            String name = FilenameUtils.getName(filePath);
            RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Vision filename parsing
            BhrutFilenameParser parser = new BhrutFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return filePath;
            }
        }

        return null;
    }
}
