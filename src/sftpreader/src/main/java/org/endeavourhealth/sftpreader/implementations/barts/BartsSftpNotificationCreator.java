package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class BartsSftpNotificationCreator extends SftpNotificationCreator {

    @Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        String relativePath = FilenameUtils.concat(dbConfiguration.getLocalRootPath(), batchSplit.getLocalRelativePath());
        String fullPath = FilenameUtils.concat(dbConfiguration.getLocalRootPathPrefix(), relativePath);

        List<String> files = findFiles(new File(fullPath), relativePath);

        return StringUtils.join(files, System.lineSeparator());
    }

    private static List<String> findFiles(File dir, String relativePath) {

        List<String> result = new ArrayList<>();

        for (File f: dir.listFiles()) {

            String newRelativePath = FilenameUtils.concat(relativePath, f.getName());

            if (f.isDirectory())
                result.addAll(findFiles(f, newRelativePath));
            else
                result.add(newRelativePath);
        }

        return result;
    }
}
