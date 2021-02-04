package org.endeavourhealth.sftpreader.implementations.homerton;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonConstants;
import org.endeavourhealth.sftpreader.implementations.homerton.utility.HomertonHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class HomertonBulkDetector extends SftpBulkDetector {


    /**
     * the Homer HI extract doesn't have any "bulk" flag, so infer whether a bulk or not by:
     * 1. look for the presence of any _delete files
     * 2. look for a small (ish) number of patients in person file (if <2000, then not a bulk)
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String personFilePath
                = HomertonHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, HomertonConstants.FILE_ID_PERSON);

        //a bulk will always contain a person file
        if (personFilePath == null) {
            return false;
        }

        //a delete file is only found in a non-bulk delta
        boolean findDeleteFile = HomertonHelper.findDeleteFileInPermDir(instanceConfiguration, dbConfiguration, batch);
        if (findDeleteFile) {
            return false;
        }

        //count up all the patient records
        Set<String> patientIds = new HashSet<>();
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(personFilePath);
        CSVParser csvParser = new CSVParser(reader, HomertonConstants.CSV_FORMAT);

        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String patientId = record.get("empi_id");
                patientIds.add(patientId);
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which also means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 2000) {
            return false;
        }

        return true;
    }

    /**
     * for Homerton, check just the main file types for the presence of patient data
     */
    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        Set<String> fileTypeIds = new HashSet<>();
        fileTypeIds.add(HomertonConstants.FILE_ID_PERSON);
        fileTypeIds.add(HomertonConstants.FILE_ID_CONDITION);
        fileTypeIds.add(HomertonConstants.FILE_ID_PROCEDURE);

        for (String fileTypeId: fileTypeIds) {

            String path = HomertonHelper.findFileInPermDir(instanceConfiguration, dbConfiguration, batch, fileTypeId);
            if (!Strings.isNullOrEmpty(path)) {
                boolean isEmpty = isFileEmpty(path, HomertonConstants.CSV_FORMAT);
                if (!isEmpty) {
                    return true;
                }
            }
        }

        return false;
    }
}
