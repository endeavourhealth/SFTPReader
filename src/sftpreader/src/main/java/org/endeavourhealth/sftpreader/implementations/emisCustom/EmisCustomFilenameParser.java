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
    private boolean isFileNeeded;

    public EmisCustomFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(isRawFile, remoteFile, dbConfiguration);
    }

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {
        String fileName = remoteFile.getFilename();

        if (fileName.equalsIgnoreCase(".bash_history")
                || fileName.equalsIgnoreCase("motd.legal-displayed")
                || fileName.equalsIgnoreCase(".viminfo")) {
            isFileNeeded = false;
            return;
        }

        if (fileName.equalsIgnoreCase("EndeavourRegistrationStatusHistory V2.7z") //raw file name
                || fileName.equalsIgnoreCase("EndeavourRegistrationStatusHistory.txt")) { //unzipped file name
            fileTypeIdentifier = "RegistrationStatus";
            extractDate = remoteFile.getLastModified().toLocalDate();
        } else {
            isFileNeeded = false;
            //throw new SftpFilenameParseException("Expecting file name: EndeavourRegistrationStatusHistory V2.7z");
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
        return isFileNeeded;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return false;
    }
}
