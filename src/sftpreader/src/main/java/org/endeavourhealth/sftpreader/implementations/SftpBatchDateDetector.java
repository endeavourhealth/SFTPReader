package org.endeavourhealth.sftpreader.implementations;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public abstract class SftpBatchDateDetector {
    private static final Logger LOG = LoggerFactory.getLogger(SftpBatchDateDetector.class);

    public abstract Date detectExtractDate(Batch batch,
                                           DataLayerI db,
                                           DbInstanceEds instanceConfiguration,
                                           DbConfiguration dbConfiguration) throws Exception;

    public abstract Date detectExtractCutoff(Batch batch,
                                           DataLayerI db,
                                           DbInstanceEds instanceConfiguration,
                                           DbConfiguration dbConfiguration) throws Exception;


    /**
     * checks the given columns in the given file, returning the most recent date found
     */
    protected Date findLatestDateFromFile(String filePath, CSVFormat csvFormat, String dateFormatStr, String... colsToCheck) throws Exception {

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath, Charset.defaultCharset());

        CSVParser csvParser = new CSVParser(reader, csvFormat);

        //date format used in SRManifest
        DateFormat dateFormat = new SimpleDateFormat(dateFormatStr);

        Date ret = null;

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                for (String colToCheck: colsToCheck) {
                    ret = findLatestDateForCell(ret, csvRecord, colToCheck, dateFormat);
                }
            }
        } finally {
            csvParser.close();
        }

        return ret;
    }

    protected Date findLatestDateForCell(Date currentLatest, CSVRecord csvRecord, String colName, DateFormat dateFormat) throws Exception {

        String val = csvRecord.get(colName);
        if (Strings.isNullOrEmpty(val)) {
            return currentLatest;
        }

        Date d = dateFormat.parse(val);
        if (currentLatest == null
                || d.after(currentLatest)) {
            return d;
        } else {
            return currentLatest;
        }
    }
}
