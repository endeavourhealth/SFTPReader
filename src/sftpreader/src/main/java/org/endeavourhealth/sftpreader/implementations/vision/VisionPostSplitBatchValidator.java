package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.time.LocalDateTime;

public class VisionPostSplitBatchValidator extends SftpPostSplitBatchValidator {
    @Override
    public void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch,
                                       DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                       DataLayerI db) throws Exception {

        //detect if we've received data out of order for Vision
        if (lastCompleteBatch != null) {
            checkForOutOfOrderBatches(incompleteBatch, lastCompleteBatch, dbConfiguration, db);
        }
    }

    @Override
    protected LocalDateTime parseBatchIdentifier(Batch batch) {
        return VisionFilenameParser.parseBatchIdentifier(batch.getBatchIdentifier());
    }


}
