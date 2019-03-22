package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpHouseKeeper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public class EmisHouseKeeper extends SftpHouseKeeper {

    private static final String RETENTION_DAYS = "EmisGpgRetentionDays";

    @Override
    public void performHouseKeeping(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //find our retention policy from the KVP table
        Integer retentionDays = null;
        for (DbConfigurationKvp dbConfigurationKvp : dbConfiguration.getDbConfigurationKvp()) {
            if (dbConfigurationKvp.getKey().equals(RETENTION_DAYS)) {
                retentionDays = Integer.parseInt(dbConfigurationKvp.getValue());
                break;
            }
        }

        //if not found, retain everything
        if (retentionDays == null) {
            return;
        }

        String configurationId = dbConfiguration.getConfigurationId();
        List<Batch> batches = db.getAllBatches(configurationId);
        for (Batch batch: batches) {

            //if not complete, skip the batch
            if (batch.getCompleteDate() == null) {
                continue;
            }

            //check it's within the retention period
            Date receivedDate = batch.getInsertDate();
            Calendar cal = Calendar.getInstance();
            cal.setTime(receivedDate);
            cal.add(Calendar.DAY_OF_YEAR, retentionDays.intValue());
            Date deleteDate = cal.getTime();
            if (deleteDate.after(new Date())) {
                continue;
            }

            for (BatchFile batchFile: batch.getBatchFiles()) {
                if (batchFile.isDeleted()) {
                    continue;
                }

                String path = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
                path = FilenameUtils.concat(path, dbConfiguration.getLocalRootPath()); //e.g. sftpReader/EMIS001
                path = FilenameUtils.concat(path, batch.getLocalRelativePath()); //e.g. 2017-04-27T09.08.00
                path = FilenameUtils.concat(path, batchFile.getFilename()); //e.g. 291_Admin_UserInRole_20150211164536_45E7CD20-EE37-41AB-90D6-DC9D4B03D102.csv.gpg

                FileHelper.deleteFromSharedStorage(path);

                batchFile.setDeleted(true);
                db.setFileAsDeleted(batchFile);
            }
        }
    }
}
