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

import java.io.StringReader;
import java.util.Collection;
import java.util.Iterator;

public abstract class SftpBulkDetector {

    public abstract boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                          DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception;

    public abstract boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                          DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception;



    protected static boolean isAllFilesEmpty(Collection<String> paths, CSVFormat csvFormat) throws Exception {

        for (String path: paths) {

            //because of how some of the fns work that call this, we may have nulls in the collection
            if (Strings.isNullOrEmpty(path)) {
                continue;
            }

            boolean isEmpty = isFileEmpty(path, csvFormat);
            if (!isEmpty) {
                return false;
            }
        }

        return true;
    }

    /**
     * reads the first 50KB from the file and attempts to parse using the given CSVFormat, returning
     * true if the file contains no data records. If the file contains a header record that's longer
     * than this, then this will not work, but 50KB should be sufficient even for the widest CDS files known
     */
    protected static boolean isFileEmpty(String path, CSVFormat csvFormat) throws Exception {
        String data = FileHelper.readFirstCharactersFromSharedStorage(path, 50 * 1024);
        if (Strings.isNullOrEmpty(data)) {
            return true;
        }

        StringReader reader = new StringReader(data);
        CSVParser parser = new CSVParser(reader, csvFormat);
        Iterator<CSVRecord> iterator = parser.iterator();
        boolean empty = !iterator.hasNext();
        parser.close();
        return empty;
    }
}
