package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;

import java.text.MessageFormat;

public class BartsSftpSlackNotifier extends SftpSlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        return getDefaultBatchMessageSuffix(completeBatch);
    }
}
