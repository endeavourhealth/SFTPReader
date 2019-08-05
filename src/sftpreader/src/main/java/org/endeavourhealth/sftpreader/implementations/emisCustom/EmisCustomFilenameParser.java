package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.implementations.barts.BartsFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDate;
import java.util.regex.Pattern;

public class EmisCustomFilenameParser extends SftpFilenameParser {

    public static final String FILE_TYPE_REG_STATUS = "RegistrationStatus";
    public static final String FILE_TYPE_ORIGINAL_TERMS = "OriginalTerms";

    //unzipped file names
    //Emis are inconsistent with naming the files, so need to use a regex to match
    private static final String REGEX_REG_STATUS = "^(?!201).*Registration(Status)?History.*(txt|7z)";
    private static final String REGEX_ORIGINAL_TERM = "^(?!201).*OriginalTerm.*(txt|7z)";
    /*public static final String FILE_NAME_REG_STATUS = "EndeavourRegistrationStatusHistory.txt";
    public static final String FILE_NAME_REG_STATUS_2 = "EndeavourRegistrationStatus.txt";
    public static final String FILE_NAME_ORIGINAL_TERMS = "EndeavourOriginalTerm.txt";*/


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
                || fileName.startsWith("201") //ignore any files I've renamed to "2018..." to archive them
                || fileName.equals("Endeavour.7z")) { //ignore failed attempt at uploading
            isFileNeeded = false;
            return;
        }

        if (isRegStatusFile(fileName)) {
            fileTypeIdentifier = FILE_TYPE_REG_STATUS;
            extractDate = remoteFile.getLastModified().toLocalDate();
            isFileNeeded = true;

        } else if (isOriginalTermFile(fileName)) {
            fileTypeIdentifier = FILE_TYPE_ORIGINAL_TERMS;
            extractDate = remoteFile.getLastModified().toLocalDate();
            isFileNeeded = true;

        } else {
            //isFileNeeded = false;
            throw new SftpFilenameParseException("Unexpected filename " + fileName);
        }
    }

    public static boolean isRegStatusFile(String fileName) {
        return Pattern.matches(REGEX_REG_STATUS, fileName);
    }

    public static boolean isOriginalTermFile(String fileName) {
        return Pattern.matches(REGEX_ORIGINAL_TERM, fileName);
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
