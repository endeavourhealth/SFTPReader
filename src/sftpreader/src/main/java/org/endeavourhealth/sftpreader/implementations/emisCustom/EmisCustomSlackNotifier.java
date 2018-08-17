package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;

public class EmisCustomSlackNotifier extends SftpSlackNotifier {
    @Override
    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        return getDefaultBatchMessageSuffix(completeBatch);
    }
}
