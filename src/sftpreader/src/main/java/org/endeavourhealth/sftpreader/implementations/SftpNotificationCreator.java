package org.endeavourhealth.sftpreader.implementations;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public abstract class SftpNotificationCreator {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpNotificationCreator.class);

    public abstract String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                                     DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception;


    /**
     * creates default notification message which is a list of files found
     */
    public String createDefaultNotificationMessage(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   BatchSplit batchSplit,
                                                   String requiredFileExtension) throws Exception {

        String sharedStoragePath = instanceConfiguration.getSharedStoragePath();
        String configurationPath = dbConfiguration.getLocalRootPath();
        String batchSplitPath = batchSplit.getLocalRelativePath();

        String path = FilenameUtils.concat(sharedStoragePath, configurationPath);
        path = FilenameUtils.concat(path, batchSplitPath);

        List<String> files = FileHelper.listFilesInSharedStorage(path);

        //we need to return the files WITHOUT the shared storage path prefix, so we need to iterate and substring
        List<String> ret = new ArrayList<>();

        for (String file: files) {
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

        return StringUtils.join(ret, System.lineSeparator());
    }
}
