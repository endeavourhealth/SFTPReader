package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class EmisSftpNotificationCreator extends SftpNotificationCreator {

    @Override
    public String createNotificationMessage(DbConfiguration dbConfiguration, BatchSplit batchSplit) {

        String combinedPath = FilenameUtils.concat(dbConfiguration.getLocalRootPath(), batchSplit.getLocalRelativePath());

        List<String> files = findFiles(new File(combinedPath), batchSplit.getLocalRelativePath());

        return StringUtils.join(files, "\r\n");
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
