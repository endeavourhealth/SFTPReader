package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.implementations.barts.BartsFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDate;

public class EmisCustomFilenameParser extends SftpFilenameParser {

    public static final String FILE_TYPE_REG_STATUS = "RegistrationStatus";
    public static final String FILE_TYPE_ORIGINAL_TERMS = "OriginalTerms";

    //unzipped file names
    public static final String FILE_NAME_REG_STATUS = "EndeavourRegistrationStatusHistory.txt";
    public static final String FILE_NAME_REG_STATUS_2 = "EndeavourRegistrationStatus.txt";
    public static final String FILE_NAME_ORIGINAL_TERMS = "EndeavourOriginalTerm.txt";


    private LocalDate extractDate;
    private String fileTypeIdentifier;
    private boolean isFileNeeded;

    public EmisCustomFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(isRawFile, remoteFile, dbConfiguration);
    }

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {
        String fileName = remoteFile.getFilename();

        //just ignore these files
        if (fileName.equalsIgnoreCase(".bash_history")
                || fileName.equalsIgnoreCase("motd.legal-displayed")
                || fileName.equalsIgnoreCase(".viminfo")
                || fileName.startsWith("201")) { //ignore any files I've renamed to "2018..." to archive them
            isFileNeeded = false;
            return;
        }

        if (fileName.equalsIgnoreCase("EndeavourRegistrationStatusHistory V2.7z") //raw file name
                || fileName.equalsIgnoreCase(FILE_NAME_REG_STATUS)
                || fileName.equalsIgnoreCase(FILE_NAME_REG_STATUS_2)) {
            fileTypeIdentifier = FILE_TYPE_REG_STATUS;
            extractDate = remoteFile.getLastModified().toLocalDate();
            isFileNeeded = true;

        } else if (fileName.equalsIgnoreCase("EndeavourOriginalTerm.7z") //raw file name
                    || fileName.equalsIgnoreCase(FILE_NAME_ORIGINAL_TERMS)) {
            fileTypeIdentifier = FILE_TYPE_ORIGINAL_TERMS;
            extractDate = remoteFile.getLastModified().toLocalDate();
            isFileNeeded = true;

        } else {
            //isFileNeeded = false;
            throw new SftpFilenameParseException("Unexpected filename " + fileName);
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
