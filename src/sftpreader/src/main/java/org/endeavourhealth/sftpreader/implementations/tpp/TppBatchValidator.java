package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;

public class TppBatchValidator extends SftpBatchValidator {

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);
    private static final String MANIFEST_FILE = "SRManifest.csv";
    private static final String REQUIRED_CHARSET = "Cp1252";

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        int batchFileCount = incompleteBatch.getBatchFiles().size();
        if (batchFileCount == 0) {
            throw new SftpValidationException("Incorrect number of files 0 in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

        //check for presence of the SRManifest.csv to signify that it is a complete batch
        List<BatchFile> batchFiles = incompleteBatch.getBatchFiles();
        boolean foundManifest = false;
        File manifestFile = null;
        for (BatchFile file: batchFiles) {

            String fileName = file.getFilename();
            if (fileName.equalsIgnoreCase(MANIFEST_FILE)) {
                foundManifest = true;
                break;
            }
        }

        //no manifest file means an incomplete batch, throw an exception so subsequent batches do not process
        if (!foundManifest) {

            throw new SftpValidationException("SRManifest.csv file missing from batch, identifier = " + incompleteBatch.getBatchIdentifier());
        }

        //read the manifest file and compare against the contents of the batch
        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String sourcePermDir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        String batchDir = incompleteBatch.getLocalRelativePath();
        sourcePermDir = FilenameUtils.concat(sourcePermDir, batchDir);

        String manifestFilePath = FilenameUtils.concat(sourcePermDir, MANIFEST_FILE);
        File f = new File(manifestFilePath);
        try {
            FileInputStream fis = new FileInputStream(f);
            BufferedInputStream bis = new BufferedInputStream(fis);
            InputStreamReader reader = new InputStreamReader(bis, Charset.forName(REQUIRED_CHARSET));
            CSVParser csvParser = new CSVParser(reader, CSV_FORMAT.withHeader());

            try {
                Iterator<CSVRecord> csvIterator = csvParser.iterator();

                while (csvIterator.hasNext()) {
                    CSVRecord csvRecord = csvIterator.next();
                    String fileName = csvRecord.get("FileName");

                    //assume the file is missing from the batch until it is found
                    boolean manifestFileFoundInBatch = false;

                    for (BatchFile file: batchFiles) {

                        String batchFileNameNoExt = file.getFilename().replace(".csv","");
                        if (fileName.equalsIgnoreCase(batchFileNameNoExt)) {
                            manifestFileFoundInBatch = false;
                            break;
                        }
                    }

                    if (!manifestFileFoundInBatch) {

                        throw new SftpValidationException("SRManifest.csv FileName: "+fileName+" missing from batch, identifier = " + incompleteBatch.getBatchIdentifier());
                    }
                }
            } finally {
                csvParser.close();
            }
        } catch (Exception ex) {
            throw new SftpValidationException(ex.getMessage());
        }

        return true;
    }
}