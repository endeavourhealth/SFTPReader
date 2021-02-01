package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.emisCustom.utility.EmisCustomConstants;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class EmisCustomBulkDetector extends SftpBulkDetector {

    /**
     * strictly speaking, the custom extracts are always bulk extracts, since they contain ALL the reg status data or ALL the original terms,
     * but we don't want to count them towards being counted as bulk extracts from the main feed, so always return false
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        return false;
    }

    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //our batch split will only have one file, the type depending on whether we've received an original term or reg status file,
        //so just list all files and check whatever we file to see if it's empty or not
        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. /sftpReader/tmp
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/EMIS_CUSTOM
        String batchSplitPath = batchSplit.getLocalRelativePath(); //e.g. 2019-02-13T08.30.35/Split/{F6F4A970-6C2D-4660-A787-0FE0E6B67DCE}

        String tempDirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchSplitPath);
        File tempDir = new File(tempDirPath);
        File[] files = tempDir.listFiles();

        for (File f: files) {
            String path = f.getAbsolutePath();
            boolean isEmpty = isFileEmpty(path, EmisCustomConstants.CSV_FORMAT.withHeader());
            if (!isEmpty) {
                return true;
            }
        }

        return false;
    }
}
