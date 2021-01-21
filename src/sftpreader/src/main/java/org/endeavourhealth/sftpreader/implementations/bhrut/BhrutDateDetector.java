package org.endeavourhealth.sftpreader.implementations.bhrut;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutConstants;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Iterator;

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
        Date aeDate = findLastestAEAttendanceDate(batch, db, instanceConfiguration, dbConfiguration);
        Date spellDate = findLastestInpatientSpellDate(batch, db, instanceConfiguration, dbConfiguration);

        Date ret = aeDate;
        if (spellDate != null
                && (ret == null || spellDate.after(ret))) {
            ret = spellDate;
        }
        return ret;
    }

    private Date findLastestInpatientSpellDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String caseFilePath = BhrutHelper.findFileInTempDir(instanceConfiguration, dbConfiguration, batch, BhrutConstants.FILE_ID_INPATIENT_SPELLS);
        if (Strings.isNullOrEmpty(caseFilePath)) {
            LOG.warn("No " + BhrutConstants.FILE_ID_INPATIENT_SPELLS + " file found in BHRUT batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        FileInputStream fis = new FileInputStream(caseFilePath);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis, Charset.defaultCharset());

        CSVFormat csvFormat = BhrutConstants.CSV_FORMAT;
        CSVParser csvParser = new CSVParser(reader, csvFormat);

        //date format used in SRManifest
        DateFormat dateFormat = new SimpleDateFormat(BhrutConstants.DATE_TIME_FORMAT);

        Date ret = null;

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                ret = findLatestDate(ret, csvRecord, "ADMISSION_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "MEDICAL_DISCHARGE_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "DISCHARGE_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "TRANSFERRED_TO_DISCHARGE_LOUNGE_DTTM", dateFormat);

            }
        } finally {
            csvParser.close();
        }

        return ret;
    }

    private Date findLastestAEAttendanceDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String caseFilePath = BhrutHelper.findFileInTempDir(instanceConfiguration, dbConfiguration, batch, BhrutConstants.FILE_ID_AE_ATTENDANCES);
        if (Strings.isNullOrEmpty(caseFilePath)) {
            LOG.warn("No " + BhrutConstants.FILE_ID_AE_ATTENDANCES + " file found in BHRUT batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        FileInputStream fis = new FileInputStream(caseFilePath);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis, Charset.defaultCharset());

        CSVFormat csvFormat = BhrutConstants.CSV_FORMAT;
        CSVParser csvParser = new CSVParser(reader, csvFormat);

        //date format used in SRManifest
        DateFormat dateFormat = new SimpleDateFormat(BhrutConstants.DATE_TIME_FORMAT);

        Date ret = null;

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                //note: not all datetime columns are checked - only those likely to contain more recent things than the arrival datetime
                ret = findLatestDate(ret, csvRecord, "ARRIVAL_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "REGISTRATION_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "TRIAGE_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "REFERRED_AE_DOCTOR_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "SEEN_BY_AE_DOCTOR_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "REFERRED_TO_PATHOLOGY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "BACK_FROM_PATHOLOGY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "REFERRED_TO_XRAY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "BACK_FROM_XRAY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "REFERRED_TO_TREATMENT_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "BACK_FROM_TREATMENT_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "LAST_REFERRED_TO_SPECIALTY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "LAST_SEEN_BY_SPECIALTY_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "BED_REQUEST_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "BED_REQUEST_OUTCOME_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "DIAGNOSIS_RECORD_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "DISCHARGED_DTTM", dateFormat);
                ret = findLatestDate(ret, csvRecord, "LEFT_DEPARTMENT_DTTM", dateFormat);

            }
        } finally {
            csvParser.close();
        }

        return ret;
    }
}
