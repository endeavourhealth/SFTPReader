package org.endeavourhealth.sftpreader.utilities;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.FileHeader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class ZipUtil {

    public static boolean validZipFile(File file, DbConfiguration dbConfiguration) throws SftpValidationException
    {
        try
        {
            // is the file a valid zip?
            ZipFile zip = new ZipFile(file.getPath());
            if (!zip.isValidZipFile())
                return false;

            // if it is a valid zip file, loop through the file headers and check for files not matching the interface fileType
            List fileHeaderList = zip.getFileHeaders();
            for (int i = 0; i < fileHeaderList.size(); i++)
            {
                FileHeader fileHeader = (FileHeader) fileHeaderList.get(i);
                String fileName = fileHeader.getFileName();
                // is the file a valid file type?
                String fileType = ValidZipFileType (dbConfiguration.getInterfaceTypeName());
                if (StringUtils.isBlank(fileType))
                    throw new SftpValidationException("Missing zip file type for interface: "+dbConfiguration.getInterfaceTypeName());

                if (!StringUtils.endsWith(fileName, fileType))
                    throw new SftpValidationException("Invalid file: "+fileName+" detected in Zip file: "+file.getPath());
            }

        } catch (ZipException e) {
            throw new SftpValidationException("Zip Exception", e);
        }

        return true;
    }

    public static List<File> unZipFile (File file, String fullRelativePath, boolean deleteZipOnCompletion) throws SftpValidationException
    {
        try {
            ZipFile zipFile = new ZipFile(file);
            //get the zip file contents

            List<File> extractFileList = new ArrayList<File>();

            List fileHeaderList = zipFile.getFileHeaders();
            for (int i = 0; i < fileHeaderList.size(); i++) {
                FileHeader fileHeader = (FileHeader) fileHeaderList.get(i);
                String fileName = fileHeader.getFileName();
                extractFileList.add(new File(fileName));
            }
            //extract all files to relative path
            zipFile.extractAll(fullRelativePath);

            //extract has worked, remove source zip file if notified
            if (deleteZipOnCompletion) {
                //remove all multi part zip parts
                if (zipFile.isSplitArchive()) {
                    ArrayList<String> zipFileParts = zipFile.getSplitZipFiles();
                    for (String fileStr : zipFileParts) {
                        FileUtils.forceDelete(new File(fileStr));
                    }
                } else {
                    FileUtils.forceDelete(file);
                }
            }

            return extractFileList;

        } catch (Exception e) {
            throw new SftpValidationException("unZipFile Exception", e);
        }
    }

    private static String ValidZipFileType (String interfaceType)
    {
        //eventually move to db lookup for interface type
        switch (interfaceType) {
            case "VISION-EXTRACT-SERVICE-1":
                return "csv";
            case "TPP-EXTRACT-SERVICE-1":
                return "csv";
            default :
                return "";
        }
    }
}
