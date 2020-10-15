package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class SftpPostSplitBatchValidator {
    private static final Logger LOG = LoggerFactory.getLogger(SftpPostSplitBatchValidator.class);

    public abstract void validateBatchPostSplit(Batch incompleteBatch, Batch lastCompleteBatch,
                                                   DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                                   DataLayerI db) throws Exception;



    /**
     * if the batch identifier (i.e. data date) for the incomplete batch is BEFORE the date of the
     * last batch then we've received data out of order. This has happened for a number of TPP extracts,
     * and a handful of Vision ones.
     */
    protected void checkForOutOfOrderBatches(Batch incompleteBatch, Batch lastCompleteBatch,
                                             DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        LocalDateTime incompleteBatchDataDate = parseBatchIdentifier(incompleteBatch);
        LocalDateTime lastCompleteBatchDataDate = parseBatchIdentifier(lastCompleteBatch);

        //Vision managed to end up with two batches with the exact same identifier, so
        //we need to change this check slightly to avoid picking them up (SD-153)
        //if (incompleteBatchDataDate.isAfter(lastCompleteBatchDataDate)) {
        if (!incompleteBatchDataDate.isBefore(lastCompleteBatchDataDate)) {
            return;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String msg = "Data received out of order for " + dbConfiguration.getConfigurationId() + "\r\n"
                + "Previous batch received on " + sdf.format(lastCompleteBatch.getInsertDate()) + " for " + lastCompleteBatch.getBatchIdentifier() + "\r\n"
                + "New batch received on " + sdf.format(incompleteBatch.getInsertDate()) + " for " + incompleteBatch.getBatchIdentifier() + "\r\n"
                + "Batches have been fixed and will be sent to Messaging API when next polling happens";
                //+ "Batches need manually ordering before sending to Messaging API";
        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

        LOG.debug("Last complete batch " + lastCompleteBatch.getBatchId() + " from " + lastCompleteBatch.getBatchIdentifier());
        LOG.debug("New incomplete batch " + incompleteBatch.getBatchId() + " from " + incompleteBatch.getBatchIdentifier());

        //the above has happened for all Vision practices, so may as well implement an automatic fix
        //rather than work it out manually.
        String configurationId = dbConfiguration.getConfigurationId();
        List<Batch> batches = db.getAllBatches(configurationId);

        //ensure batches are sorted properly
        batches.sort((o1, o2) -> {
            Integer i1 = o1.getSequenceNumber();
            Integer i2 = o2.getSequenceNumber();
            return i1.compareTo(i2);
        });

        //we need to find the lowest batch ID that needs to be fixed
        List<Batch> batchesToFix = new ArrayList<>();
        for (Batch batch: batches) {
            LocalDateTime batchDate = parseBatchIdentifier(batch);
            if (batch.getCompleteDate() != null
                && batchDate.isAfter(incompleteBatchDataDate)) {

                LOG.debug("Will need to move complete batch " + batch.getBatchId() + " from " + batch.getBatchIdentifier());
                batchesToFix.add(batch);
            }
        }

        //now we've found the batches that need fixing, then we "reset" them
        for (Batch batch: batchesToFix) {
            db.resetBatch(batch.getBatchId());
            LOG.debug("Reset batch " + batch.getBatchId());
        }

        //although we've fixed the data, we're mid-way through processing the new batch(es) so don't have
        //a way to affect the state in the main loop, so throw the exception and the now-fixed batches will
        //be picked up in the next loop
        throw new Exception(msg);
    }

    protected abstract LocalDateTime parseBatchIdentifier(Batch batch);
}
