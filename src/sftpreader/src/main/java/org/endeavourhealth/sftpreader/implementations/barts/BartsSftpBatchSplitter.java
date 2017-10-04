package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.csv.CSVFormat;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BartsSftpBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpBatchSplitter.class);

    private static final String SPLIT_COLUMN_ORG = "OrganisationGuid";
    private static final String SPLIT_COLUMN_PROCESSING_ID = "ProcessingId";

    private static final String SPLIT_FOLDER = "Split";

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT;

    /**
     * No further splitting into sub-batches for Barts
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayer db, DbConfiguration dbConfiguration) throws Exception {

        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());
        batchSplit.setOrganisationId("R1H");

        ret.add(batchSplit);

        return ret;
    }


}
