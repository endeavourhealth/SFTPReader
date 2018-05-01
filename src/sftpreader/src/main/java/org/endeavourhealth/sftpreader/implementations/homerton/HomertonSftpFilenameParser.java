package org.endeavourhealth.sftpreader.implementations.homerton;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HomertonSftpFilenameParser extends SftpFilenameParser {

    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    //private static final DateTimeFormatter FILE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");
    //private static final String SHARING_AGREEMENT_UUID_KEY = "SharingAgreementGuid";
    public static final String FILE_TYPE_PATIENT = "PATIENT";
    public static final String FILE_TYPE_PROCEDURE = "PROCEDURE";
    public static final String FILE_TYPE_DIAGNOSIS = "DIAGNOSIS";
    public static final String FILE_TYPE_PROBLEM = "PROBLEM";
    public static final String FILE_TYPE_ENCOUNTER = "ENCOUNTER";
    public static final String FILE_TYPE_CODES = "CODES";


    private String fileTypeIdentifier;
    private String fileUniqueId;
    private String batchGroup;
    private LocalDateTime extractDate;

    /*public HomertonSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }*/

    public HomertonSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return BATCH_IDENTIFIER_FORMAT.format(extractDate);
    }

    @Override
    public String generateFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

    @Override
    public boolean isFileNeeded(){
        return true;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return false;
    }

    /*@Override
    public boolean requiresDecryption() {
        return false;
    }*/

    public static String parseBatchIdentifier(String batchIdentifier) {
        return batchIdentifier;
    }

    @Override
    /*
    Parse and validate file name in order to identify:
    processingIds =             Some sort of processing-id-set / batch identifier based on file name item 1
                                Available through: getProcessingIds()
                                = Seems to be used in Validator to ensure all files in batch have same value

    file type identifier        Available through: generateFileTypeIdentifier()
                                = Used to save the value into log.batch_file
                                = Type must exist in configuration.interface_file_type

    extractDateTime             Available through Getter
                                Used by: generateBatchIdentifier()
                                Based on file name item 4
                                = Seems to be used in Validator to ensure all files in batch have same value

    sharingAgreementUuid        GUID in file name must match configuration_kvp entry
                                Available through Getter
                                Based on file name item 5 (last bit before .)
                                = Looks like it is not used outside this class
                                = Must match value in configuration.configuration_kvp
     */
    protected void parseFilename() throws SftpFilenameParseException {

        String fileName = this.remoteFile.getFilename();

        String[] parts = fileName.split("_");

        if (parts.length < 2)
            throw new SftpFilenameParseException("Homerton batch filename could not be parsed");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        extractDate = LocalDateTime.of(Integer.parseInt(filenamePart2.substring(0, 4)), Integer.parseInt(filenamePart2.substring(4, 6)), Integer.parseInt(filenamePart2.substring(6)), 0, 0);

        if (filenamePart1.compareToIgnoreCase("PATIENT") == 0) {
            fileTypeIdentifier = FILE_TYPE_PATIENT;
            batchGroup = FILE_TYPE_PATIENT;
            fileUniqueId = filenamePart2;
        } else if (filenamePart1.compareToIgnoreCase("CODES") == 0) {
            fileTypeIdentifier = FILE_TYPE_CODES;
            batchGroup = FILE_TYPE_CODES;
            fileUniqueId = filenamePart2;
        } else if (filenamePart1.compareToIgnoreCase("DIAGNOSIS") == 0) {
            fileTypeIdentifier = FILE_TYPE_DIAGNOSIS;
            batchGroup = FILE_TYPE_DIAGNOSIS;
            fileUniqueId = filenamePart2;
        } else if (filenamePart1.compareToIgnoreCase("ENCOUNTER") == 0) {
            fileTypeIdentifier = FILE_TYPE_ENCOUNTER;
            batchGroup = FILE_TYPE_ENCOUNTER;
            fileUniqueId = filenamePart2;
        } else if (filenamePart1.compareToIgnoreCase("PROBLEM") == 0) {
            fileTypeIdentifier = FILE_TYPE_PROBLEM;
            batchGroup = FILE_TYPE_PROBLEM;
            fileUniqueId = filenamePart2;
        } else if (filenamePart1.compareToIgnoreCase("PROCEDURE") == 0) {
            fileTypeIdentifier = FILE_TYPE_PROCEDURE;
            batchGroup = FILE_TYPE_PROCEDURE;
            fileUniqueId = filenamePart2;
        } else {
            throw new SftpFilenameParseException("Homerton batch filename could not be parsed");
        }
    }


}
