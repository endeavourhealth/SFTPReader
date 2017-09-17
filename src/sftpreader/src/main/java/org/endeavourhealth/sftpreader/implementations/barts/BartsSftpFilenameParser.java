package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import java.time.format.DateTimeFormatter;

public class BartsSftpFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");
    private static final String SHARING_AGREEMENT_UUID_KEY = "SharingAgreementGuid";

    private String fileTypeIdentifier;
    private String fileUniqueId;

    public BartsSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }

    public BartsSftpFilenameParser(String filename, DbConfiguration dbConfiguration) {
        super(filename, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return fileTypeIdentifier + "#" + fileUniqueId;
    }

    @Override
    public String generateFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

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
    protected void parseFilename(String filename, String pgpFileExtensionFilter) throws SftpFilenameParseException {
        String[] parts = filename.split("_");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        if (filenamePart1.compareToIgnoreCase("susopa") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = "SUSOPA";
            fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
        } else {
            if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
                if (parts.length != 2)
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                fileTypeIdentifier = "SUSAEA";
                fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
            } else {
                if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
                    if (parts.length != 2)
                        throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                    fileTypeIdentifier = filenamePart1.toUpperCase();
                    fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
                } else {
                    if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
                        if (parts.length != 2)
                            throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                        fileTypeIdentifier = filenamePart1.toUpperCase();
                        fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
                    } else {
                        String filenamePart3 = parts[2];

                        if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                            if (parts.length != 3)
                                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                            fileTypeIdentifier = filenamePart1.toUpperCase();
                            fileUniqueId = filenamePart3;
                        } else {
                            String filenamePart4 = parts[3];

                            if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                                if (parts.length != 4)
                                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                                fileTypeIdentifier = "SUSIP";
                                fileUniqueId = filenamePart3;
                            } else {
                                if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
                                    if (parts.length != 4)
                                        throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                                    fileTypeIdentifier = filenamePart3.toUpperCase();
                                    fileUniqueId = filenamePart4.substring(0, filenamePart4.indexOf("."));
                                } else {
                                    //String filenamePart5 = parts[4];
                                    String filenamePart6 = parts[5];

                                    if (filenamePart1.compareToIgnoreCase("GETL") == 0) {
                                        if (parts.length != 6)
                                            throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                                        fileTypeIdentifier = filenamePart3.toUpperCase();
                                        fileUniqueId = filenamePart6.substring(0, filenamePart6.indexOf("."));
                                    } else {
                                        throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }


}
