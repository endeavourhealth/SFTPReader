package org.endeavourhealth.sftpreader.implementations.barts;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraConstants;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraHelper;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsConstants;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
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
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class BartsDateDetector extends SftpBatchDateDetector {
    private static final Logger LOG = LoggerFactory.getLogger(BartsDateDetector.class);

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //because of the mess of CDE and CDS files, we only use a DATE for the barts batch identifer, so shouldn't just
        //use that as the extract date. For the purposes of simplicity, we look for the CDE PPATI file(s) and
        //find the extract date field from that file
        List<String> ppatiPaths = BartsHelper.findFilesInTempDir(instanceConfiguration, dbConfiguration, batch, BartsConstants.FILE_ID_CDE_PPATI);

        Date ret = null;
        DateFormat dateFormat = new SimpleDateFormat(BartsConstants.CDE_DATE_TIME_FORMAT);

        for (String ppatiPath: ppatiPaths) {

            FileInputStream fis = new FileInputStream(ppatiPath);
            BufferedInputStream bis = new BufferedInputStream(fis);
            InputStreamReader reader = new InputStreamReader(bis, Charset.defaultCharset());

            CSVFormat csvFormat = BartsConstants.CDE_CSV_FORMAT;
            CSVParser csvParser = new CSVParser(reader, csvFormat);

            try {
                Iterator<CSVRecord> csvIterator = csvParser.iterator();

                while (csvIterator.hasNext()) {
                    CSVRecord csvRecord = csvIterator.next();
                    ret = findLatestDate(ret, csvRecord, "EXTRACT_DT_TM", dateFormat);
                }
            } finally {
                csvParser.close();
            }
        }

        return ret;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //we believe that the PowerInsight data warehouse that is used for the CDE extracts is only updated at midnight, so
        //the data cutoff is the midnight before the extract date
        Date extractDate = batch.getExtractDate(); //this will already have been set if possible
        if (extractDate == null) {
            return null;
        }

        Calendar cal = Calendar.getInstance();
        cal.setTime(extractDate);
        cal.set(Calendar.MILLISECOND, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        return cal.getTime();
    }

}
