package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.implementations.barts.BartsFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class EmisCustomFilenameParser extends SftpFilenameParser {

    private LocalDate extractDate;
    private String fileTypeIdentifier;

    public EmisCustomFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
    }

    @Override
    protected void parseFilename() throws SftpFilenameParseException {
        String fileName = remoteFile.getFilename();
        if (fileName.equalsIgnoreCase("EndeavourRegistrationStatusHistory V2.7z")
                || fileName.equalsIgnoreCase("EndeavourRegistrationStatusHistory.txt")) {
            fileTypeIdentifier = "RegistrationStatus";
            extractDate = remoteFile.getLastModified().toLocalDate();
        } else {
            throw new SftpFilenameParseException("Expecting file name: EndeavourRegistrationStatusHistory V2.7z");
        }
    }

    @Override
    public String generateBatchIdentifier() {
        return BartsFilenameParser.createBatchIdentifier(this.extractDate);
    }

    @Override
    public String generateFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

    @Override
    public boolean isFileNeeded() {
        return true;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return false;
    }
}
