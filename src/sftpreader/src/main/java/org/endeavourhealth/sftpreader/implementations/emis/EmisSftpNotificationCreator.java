package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.eds.EdsSenderHttpErrorResponseException;
import org.endeavourhealth.common.eds.EdsSenderResponse;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.*;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class EmisSftpNotificationCreator extends SftpNotificationCreator {

    @Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit);
    }


    /*@Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        //if we pass the start date check, then return the payload for the notification ot the messaging API
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
    }*/
}
