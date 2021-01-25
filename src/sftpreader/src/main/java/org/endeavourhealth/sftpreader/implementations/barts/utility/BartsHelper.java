package org.endeavourhealth.sftpreader.implementations.barts.utility;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
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
     * finds multiple data files in the permanent storage
     *
     * this is similar to other functions for other publishers, but Barts sends multiples
     * of some file types, and has a wide range of file extensions
     */
    public static List<String> findFilesInPermDir(DbInstanceEds instanceConfiguration,
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
        List<String> ret = new ArrayList<>();

        for (String filePath: files) {
            String name = FilenameUtils.getName(filePath);
            RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Vision filename parsing
            BartsFilenameParser parser = new BartsFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                ret.add(filePath);
            }
        }

        return ret;
    }

}
