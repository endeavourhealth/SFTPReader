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
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.core.csv.CsvHelper;
import org.endeavourhealth.core.database.rdbms.ConnectionManager;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppRebulkFilterHelper;
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

            int done = 0;
            for (Batch b: batches) {

                List<BatchSplit> splits = dataLayer.getBatchSplitsForBatch(b.getBatchId());
                LOG.debug("Doing batch " + b.getBatchId() + " from " + b.getBatchIdentifier() + " with " + splits.size() + " splits");

                for (BatchSplit split: splits) {

                    String odsCode = split.getOrganisationId();
                    if (!Strings.isNullOrEmpty(odsCodeRegex)
                            && !Pattern.matches(odsCodeRegex, odsCode)) {
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

}
