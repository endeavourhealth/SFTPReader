package org.endeavourhealth.sftpreader.implementations.barts;

import com.google.common.base.Strings;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsConstants;
import org.endeavourhealth.sftpreader.implementations.barts.utility.BartsHelper;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BartsBulkDetector extends SftpBulkDetector {

    /**
     * we've never received a single bulk for Barts, and don't expect to get one in the future, so no need to implement this
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        return false;
    }

    /**
     * for Barts, check the key files in the CDE data only. This does mean we could fail to detect patient data if the CDE
     * files are empty and we have only CDS files, but I don't think that's possible
     */
    @Override
    public boolean hasPatientData(Batch batch, BatchSplit batchSplit, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        Set<String> fileTypeIds = new HashSet<>();
        fileTypeIds.add(BartsConstants.FILE_ID_CDE_PPATI);
        fileTypeIds.add(BartsConstants.FILE_ID_CDE_CLEVE);
        fileTypeIds.add(BartsConstants.FILE_ID_CDE_ENCNT);

        for (String fileTypeId: fileTypeIds) {

            List<String> paths = BartsHelper.findFilesInPermDir(instanceConfiguration, dbConfiguration, batch, fileTypeId);
            boolean isEmpty = isAllFilesEmpty(paths, BartsConstants.CDE_CSV_FORMAT.withHeader());
            if (!isEmpty) {
                return true;
            }
        }

        return false;
    }
}
