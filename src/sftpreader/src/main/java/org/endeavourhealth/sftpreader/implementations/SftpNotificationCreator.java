package org.endeavourhealth.sftpreader.implementations;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.common.utility.JsonSerializer;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.ExchangePayloadFile;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public abstract class SftpNotificationCreator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpNotificationCreator.class);

    public abstract PayloadWrapper createNotificationMessage(String organisationId, DataLayerI db, DbInstanceEds instanceConfiguration,
                                                     DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception;


    /**
     * creates default notification message which is a list of files found
     */
    protected PayloadWrapper createDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                      DataLayerI db,
                                                   BatchSplit batchSplit,
                                                   String requiredFileExtension) throws Exception {

        List<ExchangePayloadFile> files = findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration, db, batchSplit, requiredFileExtension);
        return new PayloadWrapper(files);
    }



    protected List<ExchangePayloadFile> findFilesForDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                                               DbConfiguration dbConfiguration,
                                                                               DataLayerI db,
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

        //we sometimes (for Barts) receive files for a given date late. These late files end up
        //being put against a new batch with the same batch identifier (which is good). But when we
        //come to notify for these late batches, the above code will have picked up all the previously
        //sent files too. So we need to exclude any file that was previously sent to the Messaging API in another batch.
        removeAlreadyNotifiedFiles(db, batchSplit, ret);

        return ret;
    }

    private void removeAlreadyNotifiedFiles(DataLayerI db, BatchSplit batchSplit, List<ExchangePayloadFile> files) throws Exception {

        Set<String> filesAlreadyNotified = new HashSet<>();

        //the previous notified messages are XML containing the notification payload in Base64 encoding. There's
        //not pretty way to get that Base64 content out, but this will work so long as the XML template isn't changed
        List<String> previousMessages = db.getNotifiedMessages(batchSplit);
        for (String xml: previousMessages) {
            String startToken = "<content value=\"";
            int startIndex = xml.indexOf(startToken);
            if (startIndex == -1) {
                throw new RuntimeException("Failed to find '" + startToken + "' in XML " + xml);
            }
            String endToken = "\"";
            int endIndex = xml.indexOf(endToken, startIndex + startToken.length());
            if (endIndex == -1) {
                throw new RuntimeException("Failed to find '" + endToken + "' in XML " + xml);
            }

            String base64 = xml.substring(startIndex + startToken.length(), endIndex);
            byte[] bytes = Base64.getDecoder().decode(base64);
            String json = new String(bytes);

            try {
                ExchangePayloadFile[] previousFiles = JsonSerializer.deserialize(json, ExchangePayloadFile[].class);
                for (ExchangePayloadFile previousFile : previousFiles) {
                    String path = previousFile.getPath();
                    filesAlreadyNotified.add(path);
                }
            } catch (Exception ex) {
                //older batches just used a list of files separated by newlines, rather than JSON
                String[] previousFiles = json.split("\n");
                for (String previousFile: previousFiles) {
                    previousFile = previousFile.trim(); //handle inconsistency between windows and linux EOF chars
                    filesAlreadyNotified.add(previousFile);
                }
            }
        }

        //now remove any file that was in a previous notification
        for (int i=files.size()-1; i>=0; i--) {
            ExchangePayloadFile file = files.get(i);
            String filePath = file.getPath();
            if (filesAlreadyNotified.contains(filePath)) {
                files.remove(i);
            }
        }
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

    public static class PayloadWrapper {
        private String payload;
        private Long totalSize;

        public PayloadWrapper(List<ExchangePayloadFile> files) {
            this.payload = combineFilesForNotificationMessage(files);
            this.totalSize = sumFilesForNotificationMessage(files);
        }

        public PayloadWrapper(String payload, Long totalSize) {
            this.payload = payload;
            this.totalSize = totalSize;
        }

        public String getPayload() {
            return payload;
        }

        public Long getTotalSize() {
            return totalSize;
        }

        public static Long sumFilesForNotificationMessage(List<ExchangePayloadFile> files) {
            long sum = 0;
            for (ExchangePayloadFile file: files) {
                sum += file.getSize().longValue();
            }
            return new Long(sum);
        }

        public static String combineFilesForNotificationMessage(List<ExchangePayloadFile> files) {
            String json = JsonSerializer.serialize(files);
            return json;
        }
    }
}
