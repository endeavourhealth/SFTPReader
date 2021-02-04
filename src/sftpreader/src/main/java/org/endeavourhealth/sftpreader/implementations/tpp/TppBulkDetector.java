package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppHelper;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionConstants;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionHelper;
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
     * the SRManifest file is supposed to tell us whether a file is a bulk or not. However, we have had
     * at least one case where the SRPatient.csv file is flagged as a bulk but it doesn't contain any data for
     * a practice included in the other files (see batch 14239 for ODS code E85745). Because of this,
     * the TPP bulk detection has been changed to rely on the content of the Patient and Code files (similar to Emis and Vision)
     * rather than the manifest file. It still checks SRManifest and will send a Slack alert if it detects a difference,
     * so this issue can be investigated in a more timely manner.
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String manifestBulkReason = detectBulkFromManifest(batch, batchSplit, db, instanceConfiguration, dbConfiguration);
        String filesLookBulkReason = detectBulkFromFileContents(batch, batchSplit, db, instanceConfiguration, dbConfiguration);

        boolean manifestIsBulk = manifestBulkReason == null;
        boolean filesLookBulk = filesLookBulkReason == null;

        //if the match, then we're good
        if (manifestIsBulk == filesLookBulk) {
            return manifestIsBulk;
        }

        String msg = "Possible discrepancy between SRManifest (bulk = " + manifestIsBulk + ") and file content (bulk = " + filesLookBulk + ") for "
                + batchSplit.getOrganisationId() + " batch " + batch.getBatchIdentifier();
        if (!manifestIsBulk) {
            msg += "\r\nSRManifest states " + manifestBulkReason;
        }
        if (!filesLookBulk) {
            msg += "\r\nData not a bulk because " + filesLookBulkReason;
        }
        msg += "\r\nManifest content bulk state will be used";
        LOG.warn(msg);

        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

        //trust the manifest
        return manifestIsBulk;
    }

    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        Set<String> fileTypeIds = new HashSet<>();
        fileTypeIds.add(TppConstants.FILE_ID_PATIENT);
        fileTypeIds.add(TppConstants.FILE_ID_EVENT);
        fileTypeIds.add(TppConstants.FILE_ID_CODE);

        for (String fileTypeId: fileTypeIds) {

            String path = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batchSplit, fileTypeId);
            if (!Strings.isNullOrEmpty(path)) {
                boolean isEmpty = isFileEmpty(path, TppConstants.CSV_FORMAT.withHeader());
                if (!isEmpty) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * we filter content from SRCode files if we've received those records before, but don't do this if it's a bulk
     * extract, so need this fn to test if the extract looks like a bulk. In this case, test both ways of checking
     * for a bulk and return true if either think it is a bulk
     */
    public static boolean isBulkExtractNoAlert(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                               DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String manifestBulkReason = detectBulkFromManifest(batch, batchSplit, db, instanceConfiguration, dbConfiguration);
        String filesLookBulkReason = detectBulkFromFileContents(batch, batchSplit, db, instanceConfiguration, dbConfiguration);

        boolean manifestIsBulk = manifestBulkReason == null;
        boolean filesLookBulk = filesLookBulkReason == null;

        return manifestIsBulk || filesLookBulk;
    }

    /**
     * returns NULL is it looked like a bulk, and a String saying why it doesn't
     */
    private static String detectBulkFromFileContents(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                               DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the files will still be in our temp storage
        String patientFilePath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batchSplit, TppConstants.FILE_ID_PATIENT);
        String codeFilePath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batchSplit, TppConstants.FILE_ID_CODE);

        //TPP extracts don't always contain all files, in which case it's definitely not a bulk
        if (patientFilePath == null && codeFilePath == null) {
            return "No SRPatient and SRCode file found";

        } else if (patientFilePath == null) {
            return "No SRPatient file found";

        } else if (codeFilePath == null) {
            return "No SRCode file found";
        }

        Set<String> patientIds = new HashSet<>();

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(patientFilePath, TppConstants.getCharset());
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
                        return "Found deleted SRPatient record";
                    }
                }
            }
        } finally {
            csvParser.close();
        }

        //just as a safety, if the patients file was really small, then it can't be a bulk
        //which means we won't accidentally count an empty file set as a bulk
        if (patientIds.size() < 900) { //test pack has 956 patients, so set the threshold below this
            return "Only " + patientIds.size() + " patients in SRPatient";
        }

        int observationRecords = 0;
        reader = FileHelper.readFileReaderFromSharedStorage(codeFilePath, TppConstants.getCharset());
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
                    return "SRCode record found for patient not in patient file";
                }

                //if we find a deleted observation record, this can't be a bulk, so return out
                if (codeFileHasDeletedColumn) {
                    String deletedStr = record.get("RemovedData");
                    if (deletedStr.equals("1")) {
                        return "Deleted SRCode record";
                    }
                }

                observationRecords ++;
            }
        } finally {
            csvParser.close();
        }

        //this 4000 number is based on the smaller practice in the Emis test pack, so it is detected as a bulk
        if (observationRecords < 10000) {
            return "Only " + observationRecords + " SRCode records";
        }

        return null;
    }

    private static String detectBulkFromManifest(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                               DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the SRManifest file will still be in our temp storage
        String manifestPath = TppHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batchSplit, TppConstants.FILE_ID_MANIFEST);
        File f = new File(manifestPath);
        if (!f.exists()) {
            throw new Exception("Failed to find manifest file " + f);
        }

        //we know that a manifest may contain a mix of bulks and deltas, but for the sake of
        //simplicity treat it as a bulk if we have SRPatient and SRCode bulk files
        boolean patientFound = false;
        boolean patientBulk = false;
        boolean codeFound = false;
        boolean codeBulk = false;


        List<ManifestRecord> records = ManifestRecord.readManifestFile(f);
        for (ManifestRecord record: records) {
            String fileName = record.getFileNameWithExtension();
            if (fileName.equals(TppConstants.PATIENT_FILE)) {
                patientFound = true;
                patientBulk = !record.isDelta();

            } else if (fileName.equals(TppConstants.CODE_FILE)) {
                codeFound = true;
                codeBulk = !record.isDelta();
            }
        }

        if (!patientFound && !codeFound) {
            return "No SRPatient or SRCode file listed";

        } else if (!patientFound) {
            return "No SRPatient file listed";

        } else if (!codeFound) {
            return "No SRCode file listed";

        } else if  (!patientBulk && !codeBulk) {
            return "SRPatient and SRCode are deltas";

        } else if (!patientBulk) {
            return "SRPatient is delta";

        } else if (!codeBulk) {
            return "SRCode is delta";

        } else {
            //indicates it IS a bulk
            return null;
        }
    }
}
