package org.endeavourhealth.sftpreader;

import com.google.common.base.Strings;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppRebulkFilterHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.regex.Pattern;

public class SRCodeLoader {
    private static final Logger LOG = LoggerFactory.getLogger(SRCodeLoader.class);

    /**
     * one-off routine to populate the hash table sftp_reader_hashes.file_record_hash from SR Code file
     */
    public static void loadTppSRCodetoHahtable(Configuration configuration, String configurationId, String odsCodeRegex) throws Exception {
        LOG.info("Starting the load for configuration ID " + configurationId + " restricting to " + odsCodeRegex);

        try {
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

            Set<String> odsCodesFinished = new HashSet<>();
            int done = 0;

            for (int i=batches.size()-1; i>=0; i--) {
                Batch b = batches.get(i);
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug("Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {

                    String odsCode = split.getOrganisationId();
                    if (!Strings.isNullOrEmpty(odsCodeRegex)
                            && !Pattern.matches(odsCodeRegex, odsCode)) {
                        continue;
                    }

                    //if we've gone back as far as we want to for this org, then skip it
                    if (odsCodesFinished.contains(odsCode)) {
                        continue;
                    }

                    String sharedStoragePath = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
                    String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS001
                    String splitPath = split.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}
                    String fileDir = FileHelper.concatFilePath(sharedStoragePath, configurationPath, splitPath);

                    //make sure there's an SRCode file for this org
                    String filePath = FileHelper.concatFilePath(fileDir, TppConstants.CODE_FILE);
                    List<String> files = FileHelper.listFilesInSharedStorage(fileDir);
                    if (files.contains(filePath)) {

                        LocalDateTime dataDate = TppFilenameParser.parseBatchIdentifier(b.getBatchIdentifier());

                        TppRebulkFilterHelper.filterFileForDuplicateData(odsCode, filePath, TppConstants.COL_ROW_IDENTIFIER_TPP, dataDate, false, edsConfiguration);
                        LOG.debug("    Updated table for " + odsCode);

                        //see if this was a bulk SRCode and if so, then don't bother going back any further for this org
                        List<FileInfo> listing = FileHelper.listFilesInSharedStorageWithInfo(fileDir);
                        for (FileInfo info: listing) {
                            String path = info.getFilePath();
                            String fileName = FilenameUtils.getName(path);
                            if (fileName.equalsIgnoreCase(TppConstants.CODE_FILE)) {
                                long size = info.getSize();
                                if (size > 150 * 1024 * 1024) {
                                    LOG.debug("    file size " + FileUtils.byteCountToDisplaySize(size) + " so will stop processing any further files for " + odsCode);
                                    odsCodesFinished.add(odsCode);
                                }
                            }
                        }

                    } else {
                        LOG.debug("    No SRCode for " + odsCode);
                    }
                }

                done ++;
                if (done % 100 == 0) {
                    LOG.debug("Done " + done + " batches of " + batches.size());
                }
            }

            LOG.debug("Finished " + done + " batches of " + batches.size());
            LOG.info("Finished the load for configuration id: " + configurationId);

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }




    public static void testTppHashTableForRandolph(Configuration configuration, String configurationId, String odsCodeRegex) {
        LOG.info("TEST The TPP Hashing for Configuration ID " + configurationId + " restricting to " + odsCodeRegex);

        try {
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

            Set<String> odsCodesFinished = new HashSet<>();
            int done = 0;

            for (int i=batches.size()-1; i>=0; i--) {
                Batch b = batches.get(i);
                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug("Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                if (b.getSequenceNumber() > 248) {
                    LOG.debug("Skipping batch " + b.getBatchId() + " as sequence number is " + b.getSequenceNumber());
                    continue;
                }

                for (BatchSplit split: splits) {

                    String odsCode = split.getOrganisationId();
                    if (!Strings.isNullOrEmpty(odsCodeRegex)
                            && !Pattern.matches(odsCodeRegex, odsCode)) {
                        continue;
                    }

                    //if we've gone back as far as we want to for this org, then skip it
                    if (odsCodesFinished.contains(odsCode)) {
                        LOG.debug("Already finished " + odsCode);
                        continue;
                    }

                    String sharedStoragePath = edsConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
                    String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS001
                    String splitPath = split.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}
                    String fileDir = FileHelper.concatFilePath(sharedStoragePath, configurationPath, splitPath);

                    //make sure there's an SRCode file for this org
                    String filePath = FileHelper.concatFilePath(fileDir, TppConstants.CODE_FILE);
                    List<String> files = FileHelper.listFilesInSharedStorage(fileDir);
                    if (files.contains(filePath)) {

                        LocalDateTime dataDate = TppFilenameParser.parseBatchIdentifier(b.getBatchIdentifier());

                        testFilterFileForDuplicateDataForRandolph(odsCode, filePath, TppConstants.COL_ROW_IDENTIFIER_TPP, dataDate, false, edsConfiguration);
                        LOG.debug("    Updated table for " + odsCode);

                        //see if this was a bulk SRCode and if so, then don't bother going back any further for this org
                        List<FileInfo> listing = FileHelper.listFilesInSharedStorageWithInfo(fileDir);
                        for (FileInfo info: listing) {
                            String path = info.getFilePath();
                            String fileName = FilenameUtils.getName(path);
                            if (fileName.equalsIgnoreCase(TppConstants.CODE_FILE)) {
                                long size = info.getSize();
                                if (size > 150 * 1024 * 1024) {
                                    LOG.debug("    file size " + FileUtils.byteCountToDisplaySize(size) + " so will stop processing any further files for " + odsCode);
                                    odsCodesFinished.add(odsCode);
                                }
                            }
                        }

                    } else {
                        LOG.debug("    No SRCode for " + odsCode);
                    }
                }

                done ++;
                if (done % 100 == 0) {
                    LOG.debug("Done " + done + " batches of " + batches.size());
                }
            }

            LOG.debug("Finished " + done + " batches of " + batches.size());
            LOG.info("Finished the load for configuration id: " + configurationId);

        } catch (Throwable t) {
            LOG.error("", t);
        }
    }



    /**
     * This method is used to filter the duplicates. The method creates hash codes for input records and compares those with database for duplication.
     * After comparision it produces the extract without any duplicates to be further processed by Message Transformer
     */
    private static void testFilterFileForDuplicateDataForRandolph(String orgId, String filePath, String uniqueKeyColumn, LocalDateTime dataDate, boolean actuallyFilterFile, DbInstanceEds instanceConfiguration) throws Exception {
        LOG.debug("Filtering file " + filePath + " actuallyFilterFile = " + actuallyFilterFile);

        //all this will take longer than a few seconds, so get a connection that's not tracked by the pool
        Connection connection = ConnectionManager.getSftpReaderHashesNonPooledConnection();
        long msStart = System.currentTimeMillis();
        String fileName = FilenameUtils.getName(filePath);
        try {
            //don't want to be waiting to commit these large transactions so turn this on
            connection.setAutoCommit(true);

            //each publishing org has its own table for storing the hashes in, so make sure that exists
            createHashTableIfNecessaryForRandolph(connection, orgId, fileName);

            //create a local file of unique IDs and hashes from the CSV file
            File hashFile = createHashFileForRandolph(filePath, uniqueKeyColumn, instanceConfiguration);
            if (hashFile == null) {
                //if the source file was empty, then the above will return null, so just drop out now
                return;
            }

            //load the file of hashes into a temporary DB table and work out which records are new/updated and which are the same
            String permTableName = getPermanentHashTableNameForRandolph(orgId, fileName);
            String tempTableName = ConnectionManager.generateTempTableName(permTableName);
            loadDataToTempTableForRandolph(connection, tempTableName, permTableName, dataDate, hashFile);

            //if actually wanting to filter the file
            if (actuallyFilterFile) {

                //find out which record IDs we want to keep
                Set<Long> hsUniqueIdsToKeep = findRecordIdsToRetainForRandolph(connection, tempTableName);

                //filter the file to retain the records we want
                filterFileForRandolph(filePath, uniqueKeyColumn, hsUniqueIdsToKeep, instanceConfiguration);
            }

            //update the permanent hash table with the latest hashes
            updatePermHashTableForRandolph(connection, permTableName, tempTableName, dataDate);

            //drop temporary table
            //dropTempTable(connection, tempTableName);

            //delete file of hashes
            //hashFile.delete();

        } catch (Exception ex) {
            LOG.error("", ex);
            throw ex;

        } finally {
            connection.setAutoCommit(false); //MUST change this back to false
            connection.close();
        }

        long msEnd = System.currentTimeMillis();
        LOG.debug("Completed filtering of " + fileName + " for " + orgId + " in " + ((msEnd - msStart) / 1000) + "s");
    }




    private static Set<Long> findRecordIdsToRetainForRandolph(Connection connection, String tempTableName) throws Exception {

        LOG.debug("Creating index on temp table");
        String sql = "CREATE INDEX ix ON " + tempTableName + " (process_record)";
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();

        LOG.trace("Looking for records to retain");
        Set<Long> ret = new HashSet<>();

        int offset = 0;
        int rows = 100000;

        while (true) {
            sql = "SELECT record_id"
                    + " FROM " + tempTableName
                    + " WHERE process_record = true"
                    + " LIMIT " + offset + ", " + rows;
            LOG.trace("Getting " + offset + " to " + (rows + offset));

            statement = connection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int read = 0;

            while (rs.next()) {
                long id = rs.getLong(1);
                ret.add(new Long(id));
                read ++;
            }

            statement.close();

            if (read < rows) {
                break;

            } else {
                offset += read;
            }
        }

        return ret;
    }

    /**
     * generate a file of unique key (i.e. RowIdentifier and unique Hash) in our temp fir
     */
    private static File createHashFileForRandolph(String filePath, String uniqueKeyColumn, DbInstanceEds instanceConfiguration) throws Exception {

        String permStoragePath = instanceConfiguration.getSharedStoragePath();
        String tempStoragePath = instanceConfiguration.getTempDirectory();
        String dirPath = FilenameUtils.getFullPath(filePath);

        //the file being processed will normally be in the temp directory, but if not, explicitly make sure we
        //write the hash file to the temp directory. It will only be found in S3 if we're running the one-off routine
        //to initially populate the hash table.
        if (dirPath.startsWith(permStoragePath)) {
            dirPath = dirPath.replace(permStoragePath, tempStoragePath);
            if (!new File(dirPath).exists()) {
                new File(dirPath).mkdirs();
            }
        }
        String hashName = "TMP_HSH_" + FilenameUtils.getName(filePath);
        String hashPath = FilenameUtils.concat(dirPath, hashName);

        File hashFile = new File(hashPath);
        LOG.trace("Starting hash calculations from " + filePath + " to " + hashPath);

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath, TppConstants.getCharset());
        CSVFormat csvFormat = TppConstants.CSV_FORMAT.withHeader();
        CSVParser csvParser = new CSVParser(reader, csvFormat);

        //find the col index of the unique key column
        Map<String, Integer> headers = csvParser.getHeaderMap();
        if (!headers.containsKey(uniqueKeyColumn)) {
            LOG.debug("Headers found: " + headers);
            throw new Exception("Couldn't find unique key " + uniqueKeyColumn + " in " + filePath);
        }
        int uniqueKeyIndex = headers.get(uniqueKeyColumn).intValue();

        HashFunction hf = Hashing.sha256();

        FileOutputStream fos = new FileOutputStream(hashFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos, TppConstants.getCharset());
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        csvFormat = CSVFormat.DEFAULT
                .withHeader("record_id", "record_hash")
                .withRecordSeparator("\r\n"); //specify the record separator so it's always the same on Windows and Linux

        CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, csvFormat);

        int done = 0;
        Iterator<CSVRecord> iterator = csvParser.iterator();
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();

            String uniqueVal = record.get(uniqueKeyColumn);
            Hasher hashser = hf.newHasher();

            int size = record.size();
            for (int i = 0; i < size; i++) {
                if (i != uniqueKeyIndex) {
                    String val = record.get(i);
                    hashser.putString(val, Charset.defaultCharset());
                }
            }

            HashCode hc = hashser.hash();
            String hashString = hc.toString();

            csvPrinter.printRecord(uniqueVal, hashString);

            done++;
            if (done % 10000 == 0) {
                LOG.debug("Done " + done);
            }
        }

        csvPrinter.close();
        csvParser.close();
        LOG.debug("Finished hash calculations for " + done + " records to " + hashFile);

        //if the CSV had no records, then there's nothing to do
        if (done == 0) {
            LOG.debug("The input file was empty so no hashes to check");
            hashFile.delete();
            return null;

        } else {
            return hashFile;
        }
    }

    /**
     * due to the huge number of rows that will be stored in the hash table, we generate a table PER publisher
     * which limits the size to only a few million rows per org
     */
    private static void createHashTableIfNecessaryForRandolph(Connection connection, String orgId, String fileName) throws Exception {

        String sql = "CREATE TABLE IF NOT EXISTS " + getPermanentHashTableNameForRandolph(orgId, fileName) + "("
                + "record_id bigint NOT NULL, "
                + "record_hash char(128) NOT NULL, "
                + "dt_last_updated datetime NOT NULL, "
                + "CONSTRAINT pk PRIMARY KEY (record_id), "
                + "INDEX ix_id_date  (record_id, dt_last_updated, record_hash)"
                + ")"; //note no point adding compression as the hashes don't compress well
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
    }

    private static String getPermanentHashTableNameForRandolph(String orgId, String fileName) {
        String baseFileName = FilenameUtils.getBaseName(fileName);
        return "TEST_" + orgId + "_" + baseFileName;
    }

    /*
     * Create / update hashes to database
     */
    private static void updatePermHashTableForRandolph(Connection connection, String permTableName, String tempTableName, LocalDateTime dataDate) throws Exception {

        LOG.trace("Creating index on record_exists");
        String sql = "CREATE INDEX ix2 ON " + tempTableName + " (record_exists, process_record)";
        Statement statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();

        //update any records that previously existed, but have a changed term
        LOG.trace("Updating records already in table");
        sql = "UPDATE " + permTableName + " t"
                + " INNER JOIN " + tempTableName + " s"
                + " ON t.record_id = s.record_id"
                + " SET t.record_hash = s.record_hash,"
                + " t.dt_last_updated = " + ConnectionManager.formatDateString(dataDate, true)
                + " WHERE s.record_exists = true"
                + " AND s.process_record = true";
        statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();

        //insert records into the target table where the staging has new records
        LOG.trace("Adding new records to table");
        sql = "INSERT IGNORE INTO " + permTableName + " (record_id, record_hash, dt_last_updated)"
                + " SELECT record_id, record_hash, " + ConnectionManager.formatDateString(dataDate, true)
                + " FROM " + tempTableName
                + " WHERE record_exists = false";
        statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();
    }


    /*
     * loads the locally stored file of hashes into a temporary table
     */
    private static void loadDataToTempTableForRandolph(Connection connection, String tempTableName, String permTableName, LocalDateTime dataDate, File hashFile) throws Exception {

        //don't use prepared statements as there's no benefit when writing to a one-off table like this
        Statement statement = null;

        //create a temporary table to load the data into
        LOG.trace("Loading " + hashFile + " into " + tempTableName);
        String sql = "CREATE TABLE " + tempTableName + " ("
                + "record_id bigint, "
                + "record_hash char(128), "
                + "record_exists boolean DEFAULT FALSE, "
                + "process_record boolean DEFAULT TRUE, "
                + "PRIMARY KEY (record_id))";
        statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();

        //bulk load temp table, adding record number as we go
        LOG.trace("Starting bulk load into " + tempTableName + "path " + hashFile.getAbsolutePath());
        sql = "LOAD DATA LOCAL INFILE '" + hashFile.getAbsolutePath().replace("\\", "\\\\") + "'"
                + " INTO TABLE " + tempTableName
                + " FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'"
                + " LINES TERMINATED BY '\\r\\n'"
                + " IGNORE 1 LINES";
        statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();

        //work out which records already exist in the target table
        LOG.trace("Finding records that exist in file_record_hash");
        String formattedDateString = ConnectionManager.formatDateString(dataDate, true);
        sql = "UPDATE " + tempTableName + " s"
                + " INNER JOIN " + permTableName + " t"
                + " ON t.record_id = s.record_id"
                + " SET s.record_exists = true, "
                + " s.process_record = IF ((t.dt_last_updated = " + formattedDateString + ") OR (s.record_hash != t.record_hash AND t.dt_last_updated < " + formattedDateString + "), true, false)";
        //the above logic goes:
        //set process record to true IF
        //    the table dt_last_updated is the same as the file being processed (i.e. if we failed when processing this batch before and are trying again)
        //OR
        //    the table dt_last_updated is older than the file being process and the hash is different (i.e. we have a new file and its hash is different)

        statement = connection.createStatement();
        statement.executeUpdate(sql);
        statement.close();
    }

    /**
     * filters the file to keep just the records specified
     */
    private static void filterFileForRandolph(String filePath, String uniqueKeyColumn, Set<Long> hsUniqueIdsToKeep, DbInstanceEds instanceConfiguration) throws Exception {

        String dirPath = FilenameUtils.getFullPath(filePath);

        //we shouldn't be attempting to filter a file already in S3, so detect and throw an exception
        String permStoragePath = instanceConfiguration.getSharedStoragePath();
        if (dirPath.startsWith(permStoragePath)) {
            throw new Exception("Attempting to filter " + filePath + " which is already in permanent storage");
        }
        String newName = "TMP_FILTERED_" + FilenameUtils.getBaseName(filePath);
        String newPath = FilenameUtils.concat(dirPath, newName);

        File newFile = new File(newPath);
        LOG.trace("Starting file filtering from " + filePath + " to " + newFile);

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath, TppConstants.getCharset());
        CSVFormat csvFormat = TppConstants.CSV_FORMAT.withHeader();
        CSVParser csvParser = new CSVParser(reader, csvFormat);

        FileOutputStream fos = new FileOutputStream(newFile);
        OutputStreamWriter osw = new OutputStreamWriter(fos, TppConstants.getCharset());
        BufferedWriter bufferedWriter = new BufferedWriter(osw);

        String[] headers = CsvSplitter.convertCsvHeaderMapToArray(csvParser.getHeaderMap());

        csvFormat = CSVFormat.DEFAULT
                .withHeader(headers)
                .withRecordSeparator("\r\n"); //specify the record separator so it's always the same on Windows and Linux

        CSVPrinter csvPrinter = new CSVPrinter(bufferedWriter, csvFormat);

        int total = 0;
        int kept = 0;
        Iterator<CSVRecord> iterator = csvParser.iterator();
        while (iterator.hasNext()) {
            CSVRecord record = iterator.next();

            String uniqueVal = record.get(uniqueKeyColumn);
            Long longVal = Long.valueOf(uniqueVal);
            if (hsUniqueIdsToKeep.contains(longVal)) {
                csvPrinter.printRecord(record);
                kept ++;
            }

            total++;
            if (total % 10000 == 0) {
                LOG.debug("Checked " + total + " rows and kept " + kept + " so far");
            }
        }

        csvPrinter.close();
        csvParser.close();

        LOG.debug("Finished filtering of " + filePath + " from " + total + " rows to " + kept);
        LOG.debug("Filtered file is " + newFile);
    }

    public static void loadTppSRCodeForRandolph2(Configuration configuration, String configurationId, String odsCode, String filePath, String dateDateStr) throws Exception {

        DbInstance instanceConfiguration = configuration.getInstanceConfiguration();
        DbInstanceEds edsConfiguration = instanceConfiguration.getEdsConfiguration();

        LocalDateTime dataDate = TppFilenameParser.parseBatchIdentifier(dateDateStr);

        testFilterFileForDuplicateDataForRandolph(odsCode, filePath, TppConstants.COL_ROW_IDENTIFIER_TPP, dataDate, true, edsConfiguration);
    }
}
