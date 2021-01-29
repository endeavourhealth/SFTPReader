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
import java.text.ParseException;
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
        List<String> ppatiPaths = BartsHelper.findFilesInPermDir(instanceConfiguration, dbConfiguration, batch, BartsConstants.FILE_ID_CDE_PPATI);

        Date ret = null;

        CSVFormat csvFormat = BartsConstants.CDE_CSV_FORMAT.withHeader();

        for (String ppatiPath: ppatiPaths) {

            Date fileDate = null;
            try {
                fileDate = findLatestDateFromFile(ppatiPath, csvFormat, BartsConstants.CDE_DATE_TIME_FORMAT, "EXTRACT_DT_TM");
            } catch (ParseException pe) {
                LOG.error("Error parsing date " + pe.getMessage());
                LOG.error("Will atttempt using Barts bulk date time format");
                fileDate = findLatestDateFromFile(ppatiPath, csvFormat, BartsConstants.CDE_BULK_DATE_TIME_FORMAT, "EXTRACT_DT_TM");
            }

            if (fileDate != null
                    && (ret == null
                    || fileDate.after(ret))) {
                ret = fileDate;
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
