package org.endeavourhealth.sftpreader.implementations;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.common.utility.JsonSerializer;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.utilities.ExchangePayloadFile;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class SftpNotificationCreator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpNotificationCreator.class);

    public abstract String createNotificationMessage(String organisationId, DataLayerI db, DbInstanceEds instanceConfiguration,
                                                     DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception;


    /**
     * creates default notification message which is a list of files found
     */
    protected String createDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   BatchSplit batchSplit,
                                                   String requiredFileExtension) throws Exception {

        List<ExchangePayloadFile> files = findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration,
                                                                    batchSplit, requiredFileExtension);
        return combineFilesForNotificationMessage(files);
    }

    protected String combineFilesForNotificationMessage(List<ExchangePayloadFile> files) {
        String json = JsonSerializer.serialize(files);
        return json;
    }

    protected List<ExchangePayloadFile> findFilesForDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                                               DbConfiguration dbConfiguration,
                                                                               BatchSplit batchSplit,
                                                                               String requiredFileExtension) throws Exception {

        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();
        String configurationPath = dbConfiguration.getLocalRootPath();
        String batchSplitPath = batchSplit.getLocalRelativePath();

        String path = FilenameUtils.concat(sharedStoragePath, configurationPath);
        path = FilenameUtils.concat(path, batchSplitPath);

        List<ExchangePayloadFile> ret = new ArrayList<>();

        //LOG.info("listFilesInSharedStorage:" + path);
        List<FileInfo> fileInfos = FileHelper.listFilesInSharedStorageWithInfo(path);

        for (FileInfo fileInfo: fileInfos) {
            //LOG.info("listFilesInSharedStorage file name returned:" + file);
            String file = fileInfo.getFilePath();
            long size = fileInfo.getSize();
            Date lastModified = fileInfo.getLastModified();

            if (!file.startsWith(sharedStoragePath)) {
                throw new Exception("File " + file + " doesn't start with expected " + sharedStoragePath);
            }

            //ignore files that aren't valid (something caused by S3 or past use of S3FS)
            String fileName = FilenameUtils.getName(file);
            if (Strings.isNullOrEmpty(fileName)) {
                LOG.info("Ignoring " + path + " as it's not a file");
                continue;
            }

            if (!Strings.isNullOrEmpty(requiredFileExtension)) {
                String fileExtension = FilenameUtils.getExtension(file);
                if (!fileExtension.equalsIgnoreCase(requiredFileExtension)) {
                    continue;
                }
            }

            //substring by the shared path PLUS ONE to remove the separator after it
            file = file.substring(sharedStoragePath.length()+1);

            //use the file name parser to work out the file type again
            LocalDateTime ldt = LocalDateTime.ofInstant(lastModified.toInstant(), ZoneId.systemDefault());
            RemoteFile remoteFile = new RemoteFile(file, size, ldt);
            SftpFilenameParser filenameParser = ImplementationActivator.createFilenameParser(false, remoteFile, dbConfiguration);
            String fileType = filenameParser.generateFileTypeIdentifier();

            ret.add(new ExchangePayloadFile(file, new Long(size), fileType));
        }

        return ret;
    }

    /*
    protected String createDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   BatchSplit batchSplit,
                                                   String requiredFileExtension) throws Exception {

        List<String> files = findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration,
                                                                    batchSplit, requiredFileExtension);
        return combineFilesForNotificationMessage(files);
    }

    protected String combineFilesForNotificationMessage(List<String> files) {
        return StringUtils.join(files, System.lineSeparator());
    }

    protected List<String> findFilesForDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                               DbConfiguration dbConfiguration,
                                                               BatchSplit batchSplit,
                                                               String requiredFileExtension) throws Exception {

        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();
        String configurationPath = dbConfiguration.getLocalRootPath();
        String batchSplitPath = batchSplit.getLocalRelativePath();

        String path = FilenameUtils.concat(sharedStoragePath, configurationPath);
        path = FilenameUtils.concat(path, batchSplitPath);

        //LOG.info("listFilesInSharedStorage:" + path);
        List<String> files = FileHelper.listFilesInSharedStorage(path);

        //we need to return the files WITHOUT the shared storage path prefix, so we need to iterate and substring
        List<String> ret = new ArrayList<>();

        for (String file: files) {
            //LOG.info("listFilesInSharedStorage file name returned:" + file);
            if (!file.startsWith(sharedStoragePath)) {
                throw new Exception("File " + file + " doesn't start with expected " + sharedStoragePath);
            }

            //ignore files that aren't valid (something caused by S3 or past use of S3FS)
            String fileName = FilenameUtils.getName(file);
            if (Strings.isNullOrEmpty(fileName)) {
                LOG.info("Ignoring " + path + " as it's not a file");
                continue;
            }

            if (!Strings.isNullOrEmpty(requiredFileExtension)) {
                String fileExtension = FilenameUtils.getExtension(file);
                if (!fileExtension.equalsIgnoreCase(requiredFileExtension)) {
                    continue;
                }
            }

            //substring by the shared path PLUS ONE to remove the separator after it
            file = file.substring(sharedStoragePath.length()+1);
            ret.add(file);
        }

        return ret;
    }*/

}
