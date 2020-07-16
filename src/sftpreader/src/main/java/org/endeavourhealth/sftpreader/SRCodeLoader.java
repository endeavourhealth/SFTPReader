package org.endeavourhealth.sftpreader;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

public class SRCodeLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SRCodeLoader.class);

    /**
     * one-off routine to populate the hash table sftp_reader_hashes.file_record_hash from SR Code file
     */
    public static void loadTPPSRCodetoHahtable(Configuration configuration, String configurationId) throws Exception {
        LOG.info("Starting the load for configuration id: " + configurationId);
        Connection conn = null;
        PreparedStatement ps = null;

        try {
            conn = ConnectionManager.getSftpReaderNonPooledConnection();

            DbConfiguration dbConfiguration = null;
            for (DbConfiguration c : configuration.getConfigurations()) {
                if (c.getConfigurationId().equals(configurationId)) {
                    dbConfiguration = c;
                    break;
                }
            }
            if (dbConfiguration == null) {
                throw new Exception("Failed to find configuration " + configurationId);
            }

            DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
            DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

            DataLayerI dataLayer = configuration.getDataLayer();
            List<Batch> batches = dataLayer.getAllBatches(configurationId);
            LOG.debug("Found " + batches.size() + " batches");

            //ensure batches are sorted properly
            batches.sort((o1, o2) -> {
                Integer i1 = o1.getSequenceNumber();
                Integer i2 = o2.getSequenceNumber();
                return i1.compareTo(i2);
            });
            int count = 0;
            for (Batch b: batches) {
                count ++;
                //if (count > 1) System.exit(0);
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug(">>>>>>>>Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {


                    String permDir = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/path
                    String tempDir = edsConfiguration.getTempDirectory(); //e.g. c:\temp
                    String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
                    String batchRelativePath = b.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00
                    String splitRelativePath = split.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

                    String tempPath = FilenameUtils.concat(tempDir, configurationDir);
                    LOG.trace("permDir: " + permDir + " tempDir: " + tempDir + " configurationDir: " + configurationDir  + " batchRelativePath: "
                            + batchRelativePath  + " splitRelativePath: " + splitRelativePath ) ;
                    tempPath = FilenameUtils.concat(tempPath, splitRelativePath);
                    boolean createdTempPath = new File(tempPath).mkdirs();

                    //for TPP, we have to copy Code file to tmp
                    List<String> files = new ArrayList<>();
                    files.add(TppConstants.CODE_FILE);

                    String permPath = FilenameUtils.concat(permDir, configurationDir);
                    permPath = FilenameUtils.concat(permPath, splitRelativePath);

                    List<String> storageContents = FileHelper.listFilesInSharedStorage(permPath);
                    File splitFile = null;
                    for (String filePermPath: storageContents) {
                        String fileName = FilenameUtils.getName(filePermPath);
                        if (files.contains(fileName)) {

                            String fileTempPath = FilenameUtils.concat(tempPath, fileName);

                            try {
                                InputStream is = FileHelper.readFileFromSharedStorage(filePermPath);
                                splitFile = new File(fileTempPath);
                                Files.copy(is, new File(fileTempPath).toPath(), StandardCopyOption.REPLACE_EXISTING);
                                is.close();
                                // Create hashes for records in SR Code and write to Hash table
                                writeToHashtable(permPath, TppConstants.COL_ROW_IDENTIFIER_TPP, splitFile, batchRelativePath);
                                //delete the tmp directory contents
                                FileHelper.deleteRecursiveIfExists(tempPath);
                            } catch (Exception ex) {
                                LOG.error("Error copying " + filePermPath + " to " + fileTempPath);
                                throw ex;
                            }
                        }
                    }

                }
            }

            LOG.info("Finished the load for configuration id: " + configurationId);
        } catch (Throwable t) {
            LOG.error("", t);
        }  finally {
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * This method is used to filter the duplicates. The method creates hash codes for input records and compares those with database for duplication.
     * After comparision it produces the extract without any duplicates to be further processed by Message Transformer
     *
     * @param storagePath This is the path where SR Code file is available for a batch split
     * @param uniqueKey This is the unique field name in the input feed file, i.e., RowIdentifier in SRCode
     * @param splitFile This is the input SR Code file
     * @param batchDir batch dir, this represents date in the file path i.e., 2017-04-26T09.37.00
     * @return Nothing.
     * @exception Exception On processing error.
     */
    private static void writeToHashtable(String storagePath, String uniqueKey,
                                         File splitFile, String batchDir) throws Exception {
        LOG.info("Hashed File Filtering for SRCode using storagePath : " + storagePath + " uniqueKey " + uniqueKey  +  " batchDir " + batchDir  );
        Connection connection = ConnectionManager.getSftpReaderHashesNonPooledConnection();
        try {
            //turn on auto commit so we don't need to separately commit these large SQL operations
            connection.setAutoCommit(true);
            //Get the date from batch directory of format i.e., 2017-04-26T09.37.00
            Date dataDate = null;
            String formattedDateString = null;
            if(batchDir != null && batchDir.length() > 0 ) {
                SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                dataDate = formatter.parse(batchDir.substring(0, batchDir.indexOf("T")));
                formattedDateString = ConnectionManager.formatDateString(dataDate, false);
            }
            else
                throw new Exception("Unexpected file " + storagePath);

            HashFunction hf = Hashing.sha256();

            //copy file to local file
            String name = FilenameUtils.getName(storagePath);
            String srcTempName = "TMP_SRC_" + name;

            File srcFile = new File(srcTempName);

            InputStream is = FileHelper.readFileFromSharedStorage(splitFile.toPath().toString());
            Files.copy(is, srcFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            is.close();
            LOG.debug("Copied " + srcFile.length() + " byte file from S3");

            long msStart = System.currentTimeMillis();

            CSVParser parser = CSVParser.parse(srcFile, Charset.defaultCharset(), CSVFormat.DEFAULT.withHeader());

            Map<String, Integer> headers = parser.getHeaderMap();
            if (!headers.containsKey(uniqueKey)) {
                LOG.debug("Headers found: " + headers);
                throw new Exception("Couldn't find unique key " + uniqueKey);
            }

            String[] headerArray = CsvHelper.getHeaderMapAsArray(parser);
            int uniqueKeyIndex = headers.get(uniqueKey).intValue();

            LOG.debug("Starting hash calculations");


            String hashTempName = "TMP_HSH_" + name;
            File hashFile = new File(hashTempName);
            FileOutputStream fos = new FileOutputStream(hashFile);
            OutputStreamWriter osw = new OutputStreamWriter(fos);
            BufferedWriter bufferedWriter = new BufferedWriter(osw);
            CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, CSVFormat.DEFAULT.withHeader("record_id", "record_hash", "dt_last_updated"));

            int done = 0;
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord record = iterator.next();

                String uniqueVal = record.get(uniqueKey);

                Hasher hashser = hf.newHasher();

                int size = record.size();
                for (int i=0; i<size; i++) {
                    if (i == uniqueKeyIndex) {
                        continue;
                    }

                    String val = record.get(i);
                    hashser.putString(val, Charset.defaultCharset());
                }

                HashCode hc = hashser.hash();
                String hashString = hc.toString();

                csvPrinter.printRecord(uniqueVal, hashString, formattedDateString);

                done ++;
                if (done % TppConstants.MAX_THRESHOLD_TPP == 0) {
                    LOG.debug("Done " + done);
                }
            }

            csvPrinter.close();
            parser.close();

            /*
             * Check if input file is empty then processing should not kick off.
             */
            if( done == 0 ) {
                LOG.debug("The input file is empty" );
                srcFile.delete();
                throw new Exception("The input file is empty");
            }

            LOG.debug("Finished hash calculations for " + done + " records to " + hashFile);

            //Save updated hashes to database
            LOG.debug("Inserting new records in target table file_record_hash");
            String sql = "LOAD DATA  INFILE '" + hashFile.getAbsolutePath().replace("\\", "\\\\") + "'"
                    + " INTO TABLE sftp_reader_hashes.file_record_hash "
                    + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\\"'"
                    + " LINES TERMINATED BY '\\r\\n'"
                    + " IGNORE 1 LINES (record_id, record_hash,dt_last_updated )";

            Statement statement = connection.createStatement(); //one-off SQL due to table name, so don't use prepared statement
            statement.executeUpdate(sql);
            statement.close();

            LOG.debug("Finished saving hashes to DB");

            //delete all files
            srcFile.delete();
            hashFile.delete();

            LOG.info("Finished  Hashed File Filtering for SRCode using " + storagePath);
        } catch (Throwable t) {
            LOG.error("", t);
        } finally {
            //MUST change this back to false
            connection.setAutoCommit(false);
            connection.close();
        }
    }
}
