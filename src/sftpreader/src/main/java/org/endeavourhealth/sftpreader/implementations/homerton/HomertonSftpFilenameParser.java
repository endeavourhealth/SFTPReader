package org.endeavourhealth.sftpreader.implementations.homerton;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class HomertonSftpFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");
    private static final String SHARING_AGREEMENT_UUID_KEY = "SharingAgreementGuid";
    public static final String FILE_TYPE_PATIENT = "PATIENT";
    public static final String FILE_TYPE_PROCEDURE = "PROCEDURE";
    public static final String FILE_TYPE_DIAGNOSIS = "DIAGNOSIS";
    public static final String FILE_TYPE_PROBLEM = "PROBLEM";

    private String fileTypeIdentifier;
    private String fileUniqueId;
    private String batchGroup;

    /*public HomertonSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }*/

    public HomertonSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return batchGroup + "#" + fileUniqueId;
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

        if (parts.length != 3)
            throw new SftpFilenameParseException("Homerton batch filename could not be parsed");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        String filenamePart3 = parts[2];
        if (filenamePart1.compareToIgnoreCase("PATIENT") == 0) {
            fileTypeIdentifier = FILE_TYPE_PATIENT;
            batchGroup = FILE_TYPE_PATIENT;
            fileUniqueId = filenamePart2 + "_" + filenamePart3;
        } else {
            if (filenamePart1.compareToIgnoreCase("PROBLEM") == 0) {
                fileTypeIdentifier = FILE_TYPE_PROBLEM;
                batchGroup = FILE_TYPE_PROBLEM;
                fileUniqueId = filenamePart2 + "_" + filenamePart3;
            } else {
                if (filenamePart1.compareToIgnoreCase("DIAGNOSIS") == 0) {
                    fileTypeIdentifier = FILE_TYPE_DIAGNOSIS;
                    batchGroup = FILE_TYPE_DIAGNOSIS;
                    fileUniqueId = filenamePart2 + "_" + filenamePart3;
                } else {
                    if (filenamePart1.compareToIgnoreCase("PROCEDURE") == 0) {
                        fileTypeIdentifier = FILE_TYPE_PROCEDURE;
                        batchGroup = FILE_TYPE_PROCEDURE;
                        fileUniqueId = filenamePart2 + "_" + filenamePart3;
                    } else {

                        throw new SftpFilenameParseException("Homerton batch filename could not be parsed");
                    }
                }
            }
        }
    }


}
