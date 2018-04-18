package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.*;

public class EmisSftpNotificationCreator extends SftpNotificationCreator {

    @Override
    public String createNotificationMessage(String organisationId, DataLayerI db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, "csv");
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
