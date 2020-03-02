package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class VisionBulkDetector extends SftpBulkDetector {

    /**
     * the Vision extract doesn't have any "bulk" flag, so infer whether a bulk or not by:
     * 1. look for deletes in the Patient file (if deletes, then not a bulk)
     * 2. look for deletes in the Journal file (if deletes, then not a bulk)
     * 3. look for Journal records that refer to patients not in the patient file (if so, then not a bulk)
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String patientFilePath = VisionHelper.findFileInTempDir(instanceConfiguration, dbConfiguration, batch, VisionHelper.PATIENT_FILE_TYPE);
        String journalFilePath = VisionHelper.findFileInTempDir(instanceConfiguration, dbConfiguration, batch, VisionHelper.JOURNAL_FILE_TYPE);

        //Vision extracts don't always contain all files, in which case it's definitely not a bulk
        if (patientFilePath == null
                || journalFilePath == null) {
            return false;
        }

        Set<String> patientIds = new HashSet<>();

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientFilePath);
        CSVParser csvParser = new CSVParser(reader, VisionHelper.CSV_FORMAT); //no headers in file

        try {
            Integer columnCount = null;
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                //Vision files don't contain headers, so we need to work out what column is what
                if (columnCount == null) {
                    columnCount = new Integer(calculateColumnCount(record));
                }

                String patientId = record.get(0); //PID is always first column
                String actionStr = record.get(columnCount.intValue()-1); //Action is always last column

                patientIds.add(patientId);

                validateActionStrValue(actionStr);

                //if we find a deleted patient record, this can't be a bulk, so return out
                if (actionStr.equalsIgnoreCase("D")) {
                    return false;
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 100) {
            return false;
        }

        reader = FileHelper.readFileReaderFromSharedStorage(journalFilePath);
        csvParser = new CSVParser(reader, VisionHelper.CSV_FORMAT); //no headers in file
        try {
            Integer columnCount = null;
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                //Vision files don't contain headers, so we need to work out what column is what
                if (columnCount == null) {
                    columnCount = new Integer(calculateColumnCount(record));
                }

                //patient ID is always first column
                String patientId = record.get(0);

                String actionStr = null;
                if (columnCount.intValue() == 23) {
                    actionStr = record.get(22);

                } else if (columnCount.intValue() == 42) {
                    actionStr = record.get(39);

                } else {
                    throw new Exception("");
                }

                //if our observtion file contains a record for a patient not in the patient file it can't be a bulk
                if (!patientIds.contains(patientId)) {
                    return false;
                }

                validateActionStrValue(actionStr);

                //if we find a deleted observation record, this can't be a bulk, so return out
                if (actionStr.equalsIgnoreCase("D")) {
                    return false;
                }
            }
        } finally {
            csvParser.close();
        }

        return true;
    }

    /**
     * since our Action values are just based on using a known column index validate that the value is
     * a known expected value, otherwise we risk this all failing if a new column is added
     */
    private static void validateActionStrValue(String actionStr) throws Exception {

        if (!actionStr.equalsIgnoreCase("I")
                && !actionStr.equalsIgnoreCase("U")
                && !actionStr.equalsIgnoreCase("D")) {
            throw new Exception("Column contains unexpected value [" + actionStr + "]");
        }
    }

    private static int calculateColumnCount(CSVRecord record) {
        int ret = 0;
        while (true) {
            try {
                record.get(ret);
                ret++;
            } catch (ArrayIndexOutOfBoundsException e) {
                //ran out of columns
                break;
            }
        }
        return ret;
    }
}
