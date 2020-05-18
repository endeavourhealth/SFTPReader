package org.endeavourhealth.sftpreader.implementations.bhrut;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class BhrutBulkDetector extends SftpBulkDetector {
    private static final Logger LOG = LoggerFactory.getLogger(BhrutBulkDetector.class);

    /**
     * the Bhrut extract doesn't have any "bulk" flag, so infer whether a bulk or not by:
     * 1. look for deletes in the Patient PMI file (if deletes exist , then not a bulk)
     * 2. look for a small (ish) number of patients in PMI file (if <1500, then not a bulk)
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String patientPMIFilePath
                = BhrutHelper.findFileInTempDir(instanceConfiguration, dbConfiguration, batch, BhrutHelper.PATIENT_PMI_FILE_TYPE);

        //a bulk will always contain a PMI file
        if (patientPMIFilePath == null) {
            return false;
        }

        //count up all the patient records whilst checking for non-Add rows
        Set<String> patientIds = new HashSet<>();
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientPMIFilePath);
        CSVParser csvParser = new CSVParser(reader, BhrutHelper.CSV_FORMAT);

        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String patientId = record.get("PAS_ID");
                patientIds.add(patientId);

                String lineStatusStr = record.get("LineStatus");
                validateLineStatusStrValue(lineStatusStr);

                //if we find a Delete or an Undelete patient record, this can't be a bulk, so return out
                if (!lineStatusStr.equalsIgnoreCase("Add")) {
                    return false;
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 1500) {
            return false;
        }

        return true;
    }


    private static void validateLineStatusStrValue(String actionStr) throws Exception {

        if (!actionStr.equalsIgnoreCase("Add")
                && !actionStr.equalsIgnoreCase("Delete")
                && !actionStr.equalsIgnoreCase("Undelete")) {
            throw new Exception("LineStatus column contains unexpected value [" + actionStr + "]");
        }
    }
}
