package org.endeavourhealth.sftpreader.implementations.adastra;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraConstants;
import org.endeavourhealth.sftpreader.implementations.adastra.utility.AdastraHelper;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionConstants;
import org.endeavourhealth.sftpreader.implementations.vision.utility.VisionHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.util.HashSet;
import java.util.Set;

public class AdastraBulkDetector extends SftpBulkDetector {

    /**
     * Adastra don't send any bulk extracts - they just send separate daily extracts going back for a few years
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        return false;
    }

    /**
     * for Adastra, check specific files
     */
    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        Set<String> fileTypeIds = new HashSet<>();
        fileTypeIds.add(AdastraConstants.FILE_ID_PATIENT);
        fileTypeIds.add(AdastraConstants.FILE_ID_CASE);

        for (String fileTypeId: fileTypeIds) {

            String path = AdastraHelper.findPostSplitFileInTempDir(instanceConfiguration, dbConfiguration, batchSplit, fileTypeId);

            if (!Strings.isNullOrEmpty(path)) {
                //the Adastra files don't include the headers, so we need to call this fn to work it out
                CSVFormat csvFormat = AdastraHelper.getCsvFormat(fileTypeId);
                boolean isEmpty = isFileEmpty(path, csvFormat);
                if (!isEmpty) {
                    return true;
                }
            }
        }

        return false;

    }
    
}
