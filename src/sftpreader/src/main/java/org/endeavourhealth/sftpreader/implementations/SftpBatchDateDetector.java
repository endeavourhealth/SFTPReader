package org.endeavourhealth.sftpreader.implementations;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;
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


    protected Date findLatestDate(Date currentLatest, CSVRecord csvRecord, String colName, DateFormat dateFormat) throws Exception {

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
