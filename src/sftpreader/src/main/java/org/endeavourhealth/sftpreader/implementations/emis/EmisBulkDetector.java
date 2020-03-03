package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class EmisBulkDetector extends SftpBulkDetector {

    /**
     * the Emis extract doesn't have any "bulk" flag, so infer whether a bulk or not by:
     * 1. look for deletes in the admin_patient file (if deletes, then not a bulk)
     * 2. look for deletes in the careRecord_observation file (if deletes, then not a bulk)
     * 3. look for observation records that refer to patients not in the patient file (if so, then not a bulk)
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the files will still be in our temp storage
        String patientFilePath = EmisHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, batchSplit, EmisConstants.ADMIN_PATIENT_FILE_TYPE);
        String observationFilePath = EmisHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, batchSplit, EmisConstants.CARE_RECORD_OBSERVATION_FILE_TYPE);

        Set<String> patientIds = new HashSet<>();

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientFilePath);
        CSVParser csvParser = new CSVParser(reader, EmisConstants.CSV_FORMAT.withHeader());
        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String patientId = record.get("PatientGuid");
                patientIds.add(patientId);

                //if we find a deleted patient record, this can't be a bulk, so return out
                String deletedStr = record.get("Deleted");
                if (deletedStr.equals("true")) {
                    return false;
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 1000) {
            return false;
        }

        int journalRecords = 0;
        reader = FileHelper.readFileReaderFromSharedStorage(observationFilePath);
        csvParser = new CSVParser(reader, EmisConstants.CSV_FORMAT.withHeader());
        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                //if our observation file contains a record for a patient not in the patient file it can't be a bulk
                String patientId = record.get("PatientGuid");
                if (!patientIds.contains(patientId)) {
                    return false;
                }

                //if we find a deleted observation record, this can't be a bulk, so return out
                String deletedStr = record.get("Deleted");
                if (deletedStr.equals("true")) {
                    return false;
                }

                journalRecords ++;
            }
        } finally {
            csvParser.close();
        }

        //this 4000 number is based on the smaller practice in the Emis test pack, so it is detected as a bulk
        if (journalRecords < 4000) {
            return false;
        }

        return true;
    }
}
