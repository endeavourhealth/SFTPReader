package org.endeavourhealth.sftpreader.implementations.homerton;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;

import java.text.MessageFormat;

public class HomertonSftpSlackNotifier extends SftpSlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {
        long totalSizeInBytes = completeBatch.
                getBatchFiles()
                .stream()
                .mapToLong(t -> t.getLocalSizeBytes())
                .sum();

        String totalSizeReadable = FileUtils.byteCountToDisplaySize(totalSizeInBytes);

        return MessageFormat.format(" with id of {0}, total file size {1}", completeBatch.getBatchIdentifier(), totalSizeReadable);
    }
}
