package org.endeavourhealth.sftpreader.implementations.adastra;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraConstants;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraHelper;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

public class AdastraDateDetector extends SftpBatchDateDetector {
    private static final Logger LOG = LoggerFactory.getLogger(AdastraDateDetector.class);

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //reverse parse the batch identifier back into a date to give us the extract datetime
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = AdastraFilenameParser.parseBatchIdentifier(batchIdentifier);
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //there is no clear indicator in Adastra data to tell us when an extract is from, and it varies from service to service (see SD-319)
        //so quickly scan through the CASE file to find the most recent datetime
        String caseFilePath = AdastraHelper.findPreSplitFileInPermDir(instanceConfiguration, dbConfiguration, batch, AdastraConstants.FILE_ID_CASE);
        if (Strings.isNullOrEmpty(caseFilePath)) {
            LOG.warn("No CASE file found in Adastra batch " + batch.getBatchId() + " so cannot calculate extract cutoff");
            return null;
        }

        CSVFormat csvFormat = AdastraHelper.getCsvFormat(AdastraConstants.FILE_ID_CASE);
        Date d = findLatestDateFromFile(caseFilePath, csvFormat, AdastraConstants.DATE_TIME_FORMAT, "StartDateTime", "EndDateTime");

        //we have some rare cases (approx 20) where all files in an Adastra extract are empty, so
        //we do not have enough information to work out any extract cutoff
        if (d == null) {
            LOG.debug("Null extract cutoff found in file " + caseFilePath + " for batch " + batch.getBatchId());
        }
        return d;
    }

}
