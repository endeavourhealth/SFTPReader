package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.implementations.emisCustom.utility.EmisCustomConstants;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class EmisCustomBatchValidator extends SftpBatchValidator {
    private static final Logger LOG = LoggerFactory.getLogger(EmisCustomBatchValidator.class);

    @Override
    public boolean validateBatch(Batch batch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        //String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the unzipped file should already be in our temp storage
        String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        sourceTempDir = FilenameUtils.concat(sourceTempDir, batchDir);

        for (File srcFileObj: new File(sourceTempDir).listFiles()) {

            if (srcFileObj.isDirectory()) {
                continue;
            }

            String srcFile = srcFileObj.getAbsolutePath();

            //the compressed 7z file will be in the same dir, so ignore that
            String extension = FilenameUtils.getExtension(srcFile);
            if (extension.equals("7z")) {
                continue;
            }

            LOG.trace("Validating " + srcFile);

            try {

                if (EmisCustomFilenameParser.isRegStatusFile(srcFileObj.getName())) {
                    validateRegStatusFile(batch, srcFile);

                } else if (EmisCustomFilenameParser.isOriginalTermFile(srcFileObj.getName())) {
                    validateOriginalTermsFile(batch, srcFile);

                } else {
                    throw new SftpValidationException("Unsupported file " + srcFileObj);
                }
            } catch (Exception ex) {
                throw new SftpValidationException("Exception validating " + srcFileObj, ex);
            }
        }

        return true;
    }

    /**
     * Emis have previously messed up the column count, so ensure it is correct for all records
     */
    private void validateRegStatusFile(Batch batch, String srcFile) throws Exception {

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(srcFile);
        CSVParser parser = new CSVParser(reader, EmisCustomConstants.CSV_FORMAT.withHeader());
        Iterator<CSVRecord> iterator = parser.iterator();

        try {
            while (iterator.hasNext()) {
                CSVRecord next = iterator.next();

                //ensure it contains all six columns (processing ID is required for accurate sorting)
                if (next.size() != 6) {
                    throw new Exception("Failed to handle record " + next + " with " + next.size() + " columns, expecting 6");
                }
            }

        } finally {
            parser.close();
        }
    }

    /**
     * The original terms is a tab-separated file that doesn't include any quoting of text fields, however there are a number
     * of records that contain new-line characters, meaning the CSV parser can't handle those records
     * So we need to pre-process the file to quote those records so the CSV Parser can handle them
     */
    private void validateOriginalTermsFile(Batch batch, String srcFile) throws Exception {

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(srcFile);
        CSVParser parser = new CSVParser(reader, EmisCustomConstants.CSV_FORMAT.withHeader());
        Iterator<CSVRecord> iterator = parser.iterator();

        String fixedSrcFile = srcFile + "FIXED";

        FileOutputStream fos = new FileOutputStream(fixedSrcFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos);
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        Map<String, Integer> headerMap = parser.getHeaderMap();
        String[] headers = new String[headerMap.size()];
        for (String headerVal: headerMap.keySet()) {
            Integer index = headerMap.get(headerVal);
            headers[index.intValue()] = headerVal;
        }

        CSVPrinter printer = new CSVPrinter(bufferedWriter, EmisCustomConstants.CSV_FORMAT.withHeader(headers));

        try {
            printer.flush(); //flush out the headers

            String[] pendingRecord = null;
            while (iterator.hasNext()) {
                CSVRecord next = iterator.next();

                if (next.size() == 5) {
                    //if a valid line, write any pending record and swap this to be our pending record
                    if (pendingRecord != null) {
                        printer.printRecord((Object[]) pendingRecord);
                    }

                    pendingRecord = new String[next.size()];
                    for (int i = 0; i < pendingRecord.length; i++) {
                        pendingRecord[i] = next.get(i);
                    }

                } else if (next.size() == 1) {
                    //if one of the invalid lines, then append to the pending record
                    String extraText = next.get(0);
                    String currentText = pendingRecord[pendingRecord.length - 1];
                    String combinedText = currentText + ", " + extraText;
                    pendingRecord[pendingRecord.length - 1] = combinedText;

                } else {
                    //no idea what this would be, but it's wrong
                    throw new Exception("Failed to handle record " + next + " with " + next.size() + " columns");
                }
            }

            //pring the final record
            if (pendingRecord != null) {
                printer.printRecord((Object[]) pendingRecord);
            }

        } finally {
            parser.close();
            printer.close();
        }

        //delete the original
        File src = new File(srcFile);
        File copy = new File(fixedSrcFile);
        if (!src.delete()) {
            throw new Exception("Failed to delete " + srcFile);
        }
        if (!copy.renameTo(src)) {
            throw new Exception("Failed to rename " + copy + " to " + src);
        }
    }
}
