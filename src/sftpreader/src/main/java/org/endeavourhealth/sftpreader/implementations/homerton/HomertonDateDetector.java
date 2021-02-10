package org.endeavourhealth.sftpreader.implementations.homerton;

import com.google.common.base.Strings;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;

public class HomertonDateDetector extends SftpBatchDateDetector {

    private static final Logger LOG = LoggerFactory.getLogger(HomertonDateDetector.class);

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //parse the batch identifier back into a date time to give us the extract date time
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = HomertonFilenameParser.parseBatchIdentifier(batchIdentifier);
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        // there is no clear indicator in Homerton data to tell us when an extract is from so scan the
        // procedure, .. files
        // to find the most recent relevant date time

        Date procedureServiceDate = findLatestProcedureDate(batch, db, instanceConfiguration, dbConfiguration);
        //Date spellDate = findLatestInpatientSpellDate(batch, db, instanceConfiguration, dbConfiguration);

        Date ret = procedureServiceDate;

        return ret;
    }

    private Date findLatestProcedureDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String procedureFilePath
                = HomertonHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, HomertonConstants.FILE_ID_PROCEDURE);
        if (Strings.isNullOrEmpty(procedureFilePath)) {
            LOG.warn("No " + HomertonConstants.FILE_ID_PROCEDURE + " file found in Homerton batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        String[] cols = {
                "service_start_dt_tm",
                "service_end_dt_tm"
        };

        return findLatestDateFromFile(procedureFilePath, HomertonConstants.CSV_FORMAT, HomertonConstants.DATE_TIME_FORMAT, cols);
    }
}