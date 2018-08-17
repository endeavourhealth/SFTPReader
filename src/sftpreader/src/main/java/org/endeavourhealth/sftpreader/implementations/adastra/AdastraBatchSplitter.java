package org.endeavourhealth.sftpreader.implementations.adastra;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AdastraBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(AdastraBatchSplitter.class);

    /**
     * For Adastra, the files in a batch are by date and time stamp for an organisation, so get orgId from the first filename
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());

        //for Adastra, the orgCode is the second piece of a file in a batch, so use the first
        List<BatchFile> batchFiles = batch.getBatchFiles();
        BatchFile firstBatchFile = batchFiles.get(0);
        String [] fileParts = firstBatchFile.getFilename().split("_");
        String orgCode = fileParts [1];

        batchSplit.setOrganisationId(orgCode);

        ret.add(batchSplit);

        return ret;
    }


}
