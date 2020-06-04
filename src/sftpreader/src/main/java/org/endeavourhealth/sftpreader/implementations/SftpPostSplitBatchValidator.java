package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class SftpPostSplitBatchValidator {

    public abstract void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch,
                                                   DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                                   DataLayerI db) throws Exception;



    /**
     * if the batch identifier (i.e. data date) for the incomplete batch is BEFORE the date of the
     * last batch then we've received data out of order. This has happened for a number of TPP extracts,
     * and a handful of Vision ones.
     */
    protected void checkForOutOfOrderBatches(Batch incompleteBatch, LocalDateTime incompleteBatchDataDate,
                                             Batch lastCompleteBatch, LocalDateTime lastCompleteBatchDataDate,
                                             DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        if (incompleteBatchDataDate.isAfter(lastCompleteBatchDataDate)) {
            return;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String msg = "Data received out of order for " + dbConfiguration.getConfigurationId() + "\r\n"
                + "Previous batch received on " + sdf.format(lastCompleteBatch.getInsertDate()) + " for " + lastCompleteBatch.getBatchIdentifier() + "\r\n"
                + "New batch received on " + sdf.format(incompleteBatch.getInsertDate()) + " for " + incompleteBatch.getBatchIdentifier() + "\r\n"
                + "Batches need manually ordering before sending to Messaging API";
        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

        //prevent any further processing
        throw new Exception(msg);

        //don't attempt to fix anything yet - wait until we get a fresh example and investigate immediately, then
        //add automatic fixing if necessary.
        //I THINK the fix would be to:
        //1. find all affected batches for the configuration
        //2. mark all batch splits as not-notified
        //3. mark the batches as non-complete
        //That should get the SFTP Reader to pick them up again and re-sequence everything, including
        //the new batch, so everything is in order and it will then notify the Messaging API with the data in the right order
    }
}
