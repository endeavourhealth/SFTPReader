package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TppBulkDetector extends SftpBulkDetector {
    private static final Logger LOG = LoggerFactory.getLogger(TppBulkDetector.class);

    /**
     * the TPP extract
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        boolean manifestIsBulk = detectBulkFromManifest(batch, batchSplit, db, instanceConfiguration, dbConfiguration);
        boolean filesLookBulk = detectBulkFromFileContents(batch, batchSplit, db, instanceConfiguration, dbConfiguration);

        //if the match, then we're good
        if (manifestIsBulk == filesLookBulk) {
            return manifestIsBulk;
        }

        String msg = "Manifest bulk = " + manifestIsBulk + " and file bulk = " + filesLookBulk
                + " for batch " + batch.getBatchId() + " in " + dbConfiguration.getConfigurationId();
        throw new Exception(msg);
    }

    private boolean detectBulkFromFileContents(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                               DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the files will still be in our temp storage
        String patientFilePath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, batchSplit, TppConstants.PATIENT_FILE_TYPE);
        String codeFilePath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, batchSplit, TppConstants.CODE_FILE_TYPE);

        //TPP extracts don't always contain all files, in which case it's definitely not a bulk
        if (patientFilePath == null
                || codeFilePath == null) {
            LOG.debug("Null patient or code file so not a bulk");
            return false;
        }

        Set<String> patientIds = new HashSet<>();

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientFilePath);
        CSVParser csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

        //some older versions don't have the the deleted column
        boolean patientFileHasDeletedColumn = csvParser.getHeaderMap().containsKey("RemovedData");

        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String patientId = record.get("RowIdentifier");
                patientIds.add(patientId);

                //if we find a deleted patient record, this can't be a bulk, so return out
                if (patientFileHasDeletedColumn) {
                    String deletedStr = record.get("RemovedData");
                    if (deletedStr.equals("1")) { //TPP use 1 and 0 for booleans
                        LOG.debug("Found deleted patient so not bulk");
                        return false;
                    }
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 900) { //test pack has 956 patients, so set the threshold below this
            LOG.debug("Only " + patientIds.size() + " patients so not bulk");
            return false;
        }

        int observationRecords = 0;
        reader = FileHelper.readFileReaderFromSharedStorage(codeFilePath);
        csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

        //some older versions don't have the the deleted column
        boolean codeFileHasDeletedColumn = csvParser.getHeaderMap().containsKey("RemovedData");

        try {
            Iterator<CSVRecord> iterator = csvParser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                //if our SRCode file contains a record for a patient not in the patient file it can't be a bulk
                String patientId = record.get("IDPatient");
                if (!patientIds.contains(patientId)) {
                    LOG.debug("SRCode for patient not in patient file so not bulk");
                    return false;
                }

                //if we find a deleted observation record, this can't be a bulk, so return out
                if (codeFileHasDeletedColumn) {
                    String deletedStr = record.get("RemovedData");
                    if (deletedStr.equals("1")) {
                        LOG.debug("Deleted SRCode so not bulk");
                        return false;
                    }
                }

                observationRecords ++;
            }
        } finally {
            csvParser.close();
        }

        //this 4000 number is based on the smaller practice in the Emis test pack, so it is detected as a bulk
        if (observationRecords < 10000) {
            //LOG.debug("Only " + observationRecords + " Observation records so not bulk");
            return false;
        }

        return true;
    }

    private boolean detectBulkFromManifest(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                               DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the SRManifest file will still be in our temp storage
        String manifestPath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, batchSplit, TppConstants.MANIFEST_FILE_TYPE);
        File f = new File(manifestPath);
        if (!f.exists()) {
            throw new Exception("Failed to find manifest file " + f);
        }

        //we know that a manifest may contain a mix of bulks and deltas, but for the sake of
        //simplicity treat it as a bulk if we have SRPatient and SRCode bulk files
        boolean patientBulk = false;
        boolean codeBulk = false;

        List<ManifestRecord> records = ManifestRecord.readManifestFile(f);
        for (ManifestRecord record: records) {
            String fileName = record.getFileNameWithExtension();
            if (fileName.equals(TppConstants.PATIENT_FILE)) {
                patientBulk = !record.isDelta();

            } else if (fileName.equals(TppConstants.CODE_FILE)) {
                codeBulk = !record.isDelta();
            }
        }

        if (patientBulk && codeBulk) {
            return true;

        } else {
            return false;
        }
    }
}
