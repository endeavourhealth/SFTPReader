package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class VisionSftpBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(VisionSftpBatchSplitter.class);

    /**
     * No further splitting into sub-batches for Vision as single practice batches
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());

        //for Vision SFTP, the last part o the remote path is the practice code which is used for the batch split
        int lastPathPartIndex = dbConfiguration.getSftpConfiguration().getRemotePath().lastIndexOf('/');
        String orgCode = dbConfiguration.getSftpConfiguration().getRemotePath().substring(lastPathPartIndex+1);
        batchSplit.setOrganisationId(orgCode);

        ret.add(batchSplit);

        return ret;
    }


}
