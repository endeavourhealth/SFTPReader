package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class TppPostSplitBatchValidator extends SftpPostSplitBatchValidator {
    @Override
    public void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch,
                                       DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                       DataLayerI db) throws Exception {

        //detect if we've received data out of order for TPP
        if (lastCompleteBatch != null) {
            LocalDateTime incompleteDt = TppFilenameParser.parseBatchIdentifier(incompleteBatch.getBatchIdentifier());
            LocalDateTime lastDt = TppFilenameParser.parseBatchIdentifier(lastCompleteBatch.getBatchIdentifier());
            checkForOutOfOrderBatches(incompleteBatch, incompleteDt, lastCompleteBatch, lastDt, dbConfiguration, db);
        }
    }

}
