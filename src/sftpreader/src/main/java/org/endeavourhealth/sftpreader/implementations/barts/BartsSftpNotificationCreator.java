package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BartsSftpNotificationCreator extends SftpNotificationCreator {
    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpNotificationCreator.class);

    @Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        //we have combined fragmented files into single larger ones, so need to exclude the fragments from our notification message
        List<String> files = super.findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration,
                                                                                batchSplit, null);

        //find all the files that we combined
        List<String> combinedFilePrefixes = new ArrayList<>();
        for (String file: files) {
            String baseName = FilenameUtils.getBaseName(file); //file name without extension
            if (baseName.endsWith(BartsSftpBatchSplitter.COMBINED)) {
                LOG.debug("Found combined file " + file);
                baseName = baseName.replace(BartsSftpBatchSplitter.COMBINED, "");
                combinedFilePrefixes.add(baseName);
            }
        }

        //for each combined file, remove the fragments that were used to create it
        for (String combinedFilePrefix: combinedFilePrefixes) {
            for (int i=files.size()-1; i>=0; i--) {
                String file = files.get(i);
                String baseName = FilenameUtils.getBaseName(file); //file name without extension
                if (baseName.startsWith(combinedFilePrefix)) {
                    LOG.debug("Removing " + file + " as it was combined");
                    files.remove(i);
                }
            }
        }

        return super.combineFilesForNotificationMessage(files);
        //return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, null);
    }

}
