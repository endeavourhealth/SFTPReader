package org.endeavourhealth.sftpreader.implementations.bhrut;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsConstants;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsHelper;
import org.endeavourhealth.sftpreader.implementations.bhrut.utility.BhrutConstants;
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
import java.util.List;
import java.util.Set;

public class BhrutBulkDetector extends SftpBulkDetector {
    private static final Logger LOG = LoggerFactory.getLogger(BhrutBulkDetector.class);

    /**
     * the Bhrut extract doesn't have any "bulk" flag, so infer whether a bulk or not by:
     * 1. look for deletes and updates in the Patient PMI file (if deletes exist , then not a bulk)
     * 2. look for a small (ish) number of patients in PMI file (if <1500, then not a bulk)
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String patientPMIFilePath = BhrutHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, BhrutConstants.FILE_ID_PMI);

        //a bulk will always contain a PMI file
        if (patientPMIFilePath == null) {
            return false;
        }

        //count up all the patient records whilst checking for non-Add rows
        Set<String> patientIds = new HashSet<>();
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientPMIFilePath);
        CSVParser csvParser = new CSVParser(reader, BhrutConstants.CSV_FORMAT);

        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String patientId = record.get("PAS_ID");
                patientIds.add(patientId);

                String dataUpdateStatusStr = record.get("DataUpdateStatus");
                validateDataUpdateStatusStrValue(dataUpdateStatusStr);

                //if we find a Delete or an Updated patient record, this can't be a bulk, so return out
                if (!dataUpdateStatusStr.equalsIgnoreCase("Added")) {
                    return false;
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which also means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 1500) {
            return false;
        }

        return true;
    }

    /**
     * for Bhrut, check just the main file types
     */
    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        Set<String> fileTypeIds = new HashSet<>();
        fileTypeIds.add(BhrutConstants.FILE_ID_PMI);
        fileTypeIds.add(BhrutConstants.FILE_ID_AE_ATTENDANCES);
        fileTypeIds.add(BhrutConstants.FILE_ID_INPATIENT_SPELLS);

        for (String fileTypeId: fileTypeIds) {

            String path = BhrutHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, fileTypeId);
            if (!Strings.isNullOrEmpty(path)) {
                boolean isEmpty = isFileEmpty(path, BhrutConstants.CSV_FORMAT.withHeader());
                if (!isEmpty) {
                    return true;
                }
            }
        }

        return false;
    }

    private static void validateDataUpdateStatusStrValue(String actionStr) throws Exception {

        if (!actionStr.equalsIgnoreCase("Added")
                && !actionStr.equalsIgnoreCase("Deleted")
                && !actionStr.equalsIgnoreCase("Updated")) {
            throw new Exception("DataUpdateStatus column contains unexpected value [" + actionStr + "]");
        }
    }
}
