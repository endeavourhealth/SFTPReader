package org.endeavourhealth.sftpreader.implementations.homerton.utility;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.homerton.HomertonFilenameParser;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.util.List;

import static org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants.HOMERTON_UNIVERSITY_HOSPITAL_ODS;
import static org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants.ROYAL_FREE_HOSPITAL_ODS;

public class HomertonHelper {

    /**
     * finds a Homerton data file in the permanent storage
     */
    public static String findFileInPermDir(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   Batch batch,
                                                   String fileIdentifier) throws Exception {

        String tempStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/HOMERTONHI
        String batchPath = batch.getLocalRelativePath(); //e.g. 2021-01-21T00.00.02

        String dirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchPath);

        List<String> files = FileHelper.listFilesInSharedStorage(dirPath);

        if (files == null || files.isEmpty()) {
            throw new SftpValidationException("Failed to find any files in " + dirPath);
        }

        for (String filePath: files) {
            String name = FilenameUtils.getName(filePath);
            RemoteFile r = new RemoteFile(name, -1, null);
            HomertonFilenameParser parser = new HomertonFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return filePath;
            }
        }
        return null;
    }

    /**
     * finds a Homerton delete file in the permanent storage
     */
    public static boolean findDeleteFileInPermDir(DbInstanceEds instanceConfiguration,
                                           DbConfiguration dbConfiguration,
                                           Batch batch) throws Exception {

        String tempStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/HOMERTONHI
        String batchPath = batch.getLocalRelativePath(); //e.g. 2021-01-21T00.00.02

        String dirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchPath);

        List<String> files = FileHelper.listFilesInSharedStorage(dirPath);

        if (files == null || files.isEmpty()) {
            throw new SftpValidationException("Failed to find any files in " + dirPath);
        }

        for (String filePath: files) {
            String fileNameNoExt = FilenameUtils.getBaseName(filePath);

            //found at least one delete file so must be a non bulk
            if (fileNameNoExt.endsWith("delete")) {
                return true;
            }
        }
        return false;
    }

    public static String getOdsFromOrgName (String orgName) {

        //At the moment, only three organisation names have been detected in the extract data
        orgName = orgName.trim();
        switch (orgName) {

            case "Homerton University Hospital" : return HOMERTON_UNIVERSITY_HOSPITAL_ODS;
            case "Royal Free Hospital p0349" : return ROYAL_FREE_HOSPITAL_ODS;
            case "Royal Free Hospital p2349" : return ROYAL_FREE_HOSPITAL_ODS;
            default : return null;
        }
    }
}