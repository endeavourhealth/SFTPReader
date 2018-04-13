package org.endeavourhealth.sftpreader.implementations;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;

public abstract class SftpSlackNotifier {
    public abstract String getCompleteBatchMessageSuffix(Batch completeBatch);

    public String getDefaultBatchMessageSuffix(Batch batch) {

        String batchIdentifier = batch.getBatchIdentifier();

        long totalSizeInBytes = 0;
        for (BatchFile batchFile: batch.getBatchFiles()) {
            totalSizeInBytes += batchFile.getRemoteSizeBytes();
        }

        String totalSizeReadable = FileUtils.byteCountToDisplaySize(totalSizeInBytes);
        return " with extract date of " + batchIdentifier + ", total size " + totalSizeReadable;
    }
}
