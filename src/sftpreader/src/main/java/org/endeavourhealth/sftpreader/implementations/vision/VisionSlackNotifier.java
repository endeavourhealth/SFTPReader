package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.io.FileUtils;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class VisionSlackNotifier extends SftpSlackNotifier {

    public String getCompleteBatchMessageSuffix(Batch completeBatch) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm");
        LocalDateTime extractDate = VisionFilenameParser.parseBatchIdentifier(completeBatch.getBatchIdentifier());

        long totalSizeInBytes = completeBatch.
                getBatchFiles()
                .stream()
                .mapToLong(t -> t.getRemoteSizeBytes())
                .sum();

        String totalSizeReadable = FileUtils.byteCountToDisplaySize(totalSizeInBytes);

        return MessageFormat.format(" with extract date of {0}, total file size {1}", extractDate.format(formatter), totalSizeReadable);
    }
}
