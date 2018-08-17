package org.endeavourhealth.sftpreader.implementations.homerton;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;

import java.text.MessageFormat;

public class HomertonSlackNotifier extends SftpSlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        return getDefaultBatchMessageSuffix(completeBatch);
    }
}
