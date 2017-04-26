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

        String fullPath = FilenameUtils.concat(dbConfiguration.getLocalRootPath(), batchSplit.getLocalRelativePath());

        List<String> files = findFiles(new File(fullPath));

        return StringUtils.join(files, "\r\n");
    }

    private static List<String> findFiles(File dir) {

        List<String> result = new ArrayList<>();

        for (File f: dir.listFiles()) {
            if (f.isDirectory())
                result.addAll(findFiles(f));
            else
                result.add(f.getPath());
        }

        return result;
    }
}
