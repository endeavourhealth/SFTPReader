package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;

import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EmisSftpSlackNotifier extends SftpSlackNotifier {
    public String getSlackMessage(String messageTemplate, Batch completeBatch) {

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MMM-yyyy HH:mm");
        LocalDateTime extractDate = EmisSftpFilenameParser.parseBatchIdentifier(completeBatch.getBatchIdentifier());

        return MessageFormat.format(messageTemplate, extractDate.format(formatter));
    }
}
