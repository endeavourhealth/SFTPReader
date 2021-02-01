package org.endeavourhealth.sftpreader.implementations.tpp.utility;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class TppHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TppHelper.class);

    /**
     * finds the file of the given "type" in the temp directory structure
     * Note that although this looks similar to the equivalent Emis and Vision functions, it is different since
     * all three suppliers send data differently
     */
    public static String findPostSplitFileInTempDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. /sftpReader/tmp
        return findPostSplitFileInDir(tempStoragePath, dbConfiguration, batchSplit, fileIdentifier);
    }

    public static String findPostSplitFileInPermDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String permStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/root
        return findPostSplitFileInDir(permStoragePath, dbConfiguration, batchSplit, fileIdentifier);
    }

    private static String findPostSplitFileInDir(String topLevelDir,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/TPP/YDDH3_08W
        String batchSplitPath = batchSplit.getLocalRelativePath(); //e.g. 2019-12-09T00.15.00/Split/E87065/

        String dirPath = FileHelper.concatFilePath(topLevelDir, configurationPath, batchSplitPath);

        List<String> filePaths = FileHelper.listFilesInSharedStorage(dirPath);

        for (String filePath: filePaths) {

            RemoteFile r = new RemoteFile(filePath, -1, null); //size and modification date aren't needed for TPP filename parsing
            TppFilenameParser parser = new TppFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return filePath;
            }
        }

        return null;
    }
}
