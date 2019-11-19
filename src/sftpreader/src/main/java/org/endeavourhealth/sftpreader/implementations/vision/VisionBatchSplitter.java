package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class VisionBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(VisionBatchSplitter.class);

    /**
     * No further splitting into sub-batches for Vision as single practice batches
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());

        //using the remote path to find the ODS code doesn't work on Windows machines, as they have a different path separator,
        //so pull the ODS code out of the file names instead
        List<BatchFile> files = batch.getBatchFiles();
        BatchFile firstFile = files.get(0);
        String fileName = firstFile.getFilename();
        String[] majorParts = fileName.split("-", 3);
        String[] minorParts = majorParts[0].split("_");
        String odsCode = minorParts[2];
        batchSplit.setOrganisationId(odsCode);

        //for Vision SFTP, the last part o the remote path is the practice code which is used for the batch split
        /*int lastPathPartIndex = dbConfiguration.getSftpConfiguration().getRemotePath().lastIndexOf('/');
        String orgCode = dbConfiguration.getSftpConfiguration().getRemotePath().substring(lastPathPartIndex+1);
        batchSplit.setOrganisationId(orgCode);*/

        ret.add(batchSplit);

        return ret;
    }


}
