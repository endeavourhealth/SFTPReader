package org.endeavourhealth.sftpreader.implementations.bhrut;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutConstants;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;

public class BhrutDateDetector extends SftpBatchDateDetector {
    private static final Logger LOG = LoggerFactory.getLogger(BhrutDateDetector.class);

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //reverse parse the batch identifier back into a date to give us the extract datetime
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = BhrutFilenameParser.parseBatchIdentifier(batchIdentifier);
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //there is no clear indicator in BHRUT data to tell us when an extract is from so scan the INPATIENT SPELLS and A&E ATTENDANCE files
        //to find the most recent date time
        Date aeDate = findLatestAEAttendanceDate(batch, db, instanceConfiguration, dbConfiguration);
        Date spellDate = findLatestInpatientSpellDate(batch, db, instanceConfiguration, dbConfiguration);

        Date ret = aeDate;
        if (spellDate != null
                && (ret == null || spellDate.after(ret))) {
            ret = spellDate;
        }
        return ret;
    }

    private Date findLatestInpatientSpellDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String inpatientFilePath = BhrutHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, BhrutConstants.FILE_ID_INPATIENT_SPELLS);
        if (Strings.isNullOrEmpty(inpatientFilePath)) {
            LOG.warn("No " + BhrutConstants.FILE_ID_INPATIENT_SPELLS + " file found in BHRUT batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        CSVFormat csvFormat = BhrutConstants.CSV_FORMAT.withHeader();

        String[] cols = {
                "ADMISSION_DTTM",
                "MEDICAL_DISCHARGE_DTTM",
                "DISCHARGE_DTTM",
                "TRANSFERRED_TO_DISCHARGE_LOUNGE_DTTM"
        };

        return findLatestDateFromFile(inpatientFilePath, csvFormat, BhrutConstants.DATE_TIME_FORMAT, cols);
    }

    private Date findLatestAEAttendanceDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String attendanceFilePath = BhrutHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, BhrutConstants.FILE_ID_AE_ATTENDANCES);
        if (Strings.isNullOrEmpty(attendanceFilePath)) {
            LOG.warn("No " + BhrutConstants.FILE_ID_AE_ATTENDANCES + " file found in BHRUT batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        CSVFormat csvFormat = BhrutConstants.CSV_FORMAT.withHeader();

        String[] cols = {
                "ARRIVAL_DTTM",
                "REGISTRATION_DTTM",
                "TRIAGE_DTTM",
                "REFERRED_AE_DOCTOR_DTTM",
                "SEEN_BY_AE_DOCTOR_DTTM",
                "REFERRED_TO_PATHOLOGY_DTTM",
                "BACK_FROM_PATHOLOGY_DTTM",
                "REFERRED_TO_XRAY_DTTM",
                "BACK_FROM_XRAY_DTTM",
                "REFERRED_TO_TREATMENT_DTTM",
                "BACK_FROM_TREATMENT_DTTM",
                "LAST_REFERRED_TO_SPECIALTY_DTTM",
                "LAST_SEEN_BY_SPECIALTY_DTTM",
                "BED_REQUEST_DTTM",
                "BED_REQUEST_OUTCOME_DTTM",
                "DIAGNOSIS_RECORD_DTTM",
                "DISCHARGED_DTTM",
                "LEFT_DEPARTMENT_DTTM"
        };

        return findLatestDateFromFile(attendanceFilePath, csvFormat, BhrutConstants.DATE_TIME_FORMAT, cols);
    }
}
