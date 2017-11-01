package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

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
    public String createNotificationMessage(String organisationId, DataLayer db, DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        //find the start date we've set for this organisation in the emis org map
        Date startDate = db.findEmisOrgStartDateFromOdsCode(organisationId);
        if (startDate == null) {
            throw new Exception("Organisation start date not set for " + organisationId + " in emis_organisation_map table");
        }

        Batch batch = batchSplit.getBatch();
        String batchDateStr = batch.getBatchIdentifier();
        LocalDateTime localDateTime = EmisSftpFilenameParser.parseBatchIdentifier(batchDateStr);
        ZonedDateTime zonedDateTime = localDateTime.atZone(ZoneId.systemDefault());
        Date batchDate = Date.from(zonedDateTime.toInstant());

        //if the batch date is BEFORE the start date, then return null to indicate we don't want to send on to the messaging API
        if (batchDate.before(startDate)) {
            return null;
        }

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
    }
}
