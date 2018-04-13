package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TppSftpSlackNotifier extends SftpSlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        return getDefaultBatchMessageSuffix(completeBatch);
    }
}
