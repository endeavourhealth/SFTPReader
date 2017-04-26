package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.db.Batch;

public abstract class SftpSlackNotifier {
    public abstract String getCompleteBatchMessageSuffix(Batch completeBatch);
}
