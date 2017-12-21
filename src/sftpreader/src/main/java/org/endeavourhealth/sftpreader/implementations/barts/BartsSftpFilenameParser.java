package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import java.time.format.DateTimeFormatter;

public class BartsSftpFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");
    private static final String SHARING_AGREEMENT_UUID_KEY = "SharingAgreementGuid";
    public static final String FILE_TYPE_SUSOPA = "SUSOPA";
    public static final String FILE_TYPE_SUSAEA = "SUSAEA";
    public static final String FILE_TYPE_SUSIP = "SUSIP";
    public static final String FILE_TYPE_TAILIP = "TAILIP";
    public static final String FILE_TYPE_TAILOPA = "TAILOPA";
    public static final String FILE_TYPE_TAILAEA = "TAILAEA";

    public static final String FILE_TYPE_BULK_SUSOPA = "BULKSUSOPA";
    public static final String FILE_TYPE_BULK_SUSAEA = "BULKSUSAEA";
    public static final String FILE_TYPE_BULK_SUSIP = "BULKSUSIP";
    public static final String FILE_TYPE_BULK_TAILIP = "BULKTAILIP";
    public static final String FILE_TYPE_BULK_TAILOPA = "BULKTAILOPA";
    public static final String FILE_TYPE_BULK_TAILAEA = "BULKTAILAEA";
    public static final String FILE_TYPE_BULK_BIRTH = "BULKBIRTH";
    public static final String FILE_TYPE_BULK_PREGNANCY = "BULKPREGNANCY";
    public static final String FILE_TYPE_BULK_PROBLEMS= "BULKPROBLEM";
    public static final String FILE_TYPE_BULK_DIAGNOSIS = "BULKDIAGNOSIS";
    public static final String FILE_TYPE_BULK_PROCEDURE = "BULKPROCEDURE";

    public static final String BATCH_GROUP_BULK_MATERNITY= "BULKMATERNITY";

    private String fileTypeIdentifier;
    private String fileUniqueId;
    private String batchGroup;

    public BartsSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }

    public BartsSftpFilenameParser(String filename, DbConfiguration dbConfiguration) {
        super(filename, dbConfiguration);
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
        return true;
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

        if (parts.length < 2)
            throw new SftpFilenameParseException("Barts batch filename could not be parsed");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        if (filenamePart1.compareToIgnoreCase("susopa") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_SUSOPA;
            batchGroup = FILE_TYPE_SUSOPA;
            fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);

        } else if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_SUSAEA;
            fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
            batchGroup = FILE_TYPE_SUSAEA;

        } else if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_TAILOPA;
            fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
            batchGroup = FILE_TYPE_SUSOPA;

        } else if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_TAILAEA;
            fileUniqueId = filenamePart2.substring(filenamePart2.indexOf(".") + 1);
            batchGroup = FILE_TYPE_SUSAEA;

        } else if ((filenamePart1.compareToIgnoreCase("PC") == 0) || (filenamePart1.compareToIgnoreCase("MAT") == 0) || (filenamePart1.compareToIgnoreCase("CDS") == 0)) {
            // Bulk files
            parseBulkFilename(filename, pgpFileExtensionFilter);


        } else {
            String filenamePart3 = parts[2];

            if (filenamePart1.compareToIgnoreCase("tailip") == 0) {
                if (parts.length != 3)
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                fileTypeIdentifier = FILE_TYPE_TAILIP;
                fileUniqueId = filenamePart2.split("\\.")[1];
                batchGroup = FILE_TYPE_SUSIP;
            } else {
                String filenamePart4 = parts[3];

                if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                    if (parts.length != 4)
                        throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                    fileTypeIdentifier = FILE_TYPE_SUSIP;
                    fileUniqueId = filenamePart3;
                    batchGroup = FILE_TYPE_SUSIP;
                } else {
                    if (filenamePart1.compareToIgnoreCase("rnj") == 0) {
                        if (parts.length != 4)
                            throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                        fileTypeIdentifier = filenamePart3.toUpperCase();
                        fileUniqueId = filenamePart4.substring(0, filenamePart4.indexOf("."));
                        batchGroup = fileTypeIdentifier;
                    } else {
                        //String filenamePart5 = parts[4];
                        String filenamePart6 = parts[5];

                        if (filenamePart1.compareToIgnoreCase("GETL") == 0) {
                            if (parts.length != 6)
                                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                            fileTypeIdentifier = filenamePart3.toUpperCase();
                            fileUniqueId = filenamePart4;
                            batchGroup = "MATERNITY";
                        } else {
                            throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                        }
                    }
                }
            }
        }
    }

    /*
     *
     */
    protected void parseBulkFilename(String filename, String pgpFileExtensionFilter) throws SftpFilenameParseException {
        String[] parts = filename.split("_");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];
        String filenamePart3 = parts[2];
        String filenamePart4 = parts[3];

        if (filenamePart1.compareToIgnoreCase("MAT") == 0) {
            // Bulk maternity files
            if (parts.length != 4) {
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            }
            if (filenamePart2.compareToIgnoreCase("BIRTH") == 0) {
                fileTypeIdentifier = FILE_TYPE_BULK_BIRTH;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = BATCH_GROUP_BULK_MATERNITY;
            } else if (filenamePart2.compareToIgnoreCase("PREGNANCY") == 0) {
                fileTypeIdentifier = FILE_TYPE_BULK_PREGNANCY;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = BATCH_GROUP_BULK_MATERNITY;
            } else {
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            }
        } else  if (filenamePart1.compareToIgnoreCase("PC") == 0) {
            // Bulk SNOMED files
            if (parts.length != 4) {
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            }
            if (filenamePart2.compareToIgnoreCase("PROBLEMS") == 0) {
                fileTypeIdentifier = FILE_TYPE_BULK_PROBLEMS;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_PROBLEMS;
            }
            else if (filenamePart2.compareToIgnoreCase("DIAGNOSES") == 0) {
                fileTypeIdentifier = FILE_TYPE_BULK_DIAGNOSIS;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_DIAGNOSIS;
            }
            else if (filenamePart2.compareToIgnoreCase("PROCEDURES") == 0) {
                fileTypeIdentifier = FILE_TYPE_BULK_PROCEDURE;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_PROCEDURE;
            }else {
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            }

        } else if (filenamePart1.compareToIgnoreCase("CDS") == 0) {
            // Bulk SUS files
            String filenamePart5 = parts[4];

            if (filename.toUpperCase().startsWith("CDS_OP_ALL_TAIL_")) {
                // Yes - file naming convention for this particular files is different
                if (parts.length != 6) {
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                }
                fileTypeIdentifier = FILE_TYPE_BULK_TAILOPA;
                fileUniqueId = filenamePart5.substring(0, 4) + ConvertMMMtoMM(filenamePart5.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSOPA;
            }
            else if (filename.toUpperCase().startsWith("CDS_AEA_TAIL_")) {
                fileTypeIdentifier = FILE_TYPE_BULK_TAILAEA;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSAEA;
            }
            else if (filename.toUpperCase().startsWith("CDS_APC_TAIL_")) {
                fileTypeIdentifier = FILE_TYPE_BULK_TAILIP;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSIP;
            }
            else if (filename.toUpperCase().startsWith("CDS_OPA_")) {
                if (parts.length != 4) {
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                }
                fileTypeIdentifier = FILE_TYPE_BULK_SUSOPA;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSOPA;
            }
            else if (filename.toUpperCase().startsWith("CDS_AEA_")) {
                if (parts.length != 4) {
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                }
                fileTypeIdentifier = FILE_TYPE_BULK_SUSAEA;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSAEA;
            }
            else if (filename.toUpperCase().startsWith("CDS_APC_")) {
                if (parts.length != 4) {
                    throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                }
                fileTypeIdentifier = FILE_TYPE_BULK_SUSIP;
                fileUniqueId = filenamePart3.substring(0, 4) + ConvertMMMtoMM(filenamePart3.substring(4));
                batchGroup = FILE_TYPE_BULK_SUSIP;
            }
             else
            {
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            }
        }else {
            throw new SftpFilenameParseException("Barts batch filename could not be parsed");
        }
    }

    /*
     *
     */
    private String ConvertMMMtoMM(String shortMonth) {
        if (shortMonth.compareToIgnoreCase("Jan") == 0) {
            return "01";
        }
        else if (shortMonth.compareToIgnoreCase("Feb") == 0) {
            return "02";
        }
        else if (shortMonth.compareToIgnoreCase("Mar") == 0) {
            return "03";
        }
        else if (shortMonth.compareToIgnoreCase("Apr") == 0) {
            return "04";
        }
        else if (shortMonth.compareToIgnoreCase("May") == 0) {
            return "05";
        }
        else if (shortMonth.compareToIgnoreCase("Jun") == 0) {
            return "06";
        }
        else if (shortMonth.compareToIgnoreCase("Jul") == 0) {
            return "07";
        }
        else if (shortMonth.compareToIgnoreCase("Aug") == 0) {
            return "08";
        }
        else if (shortMonth.compareToIgnoreCase("Sep") == 0) {
            return "09";
        }
        else if (shortMonth.compareToIgnoreCase("Oct") == 0) {
            return "10";
        }
        else if (shortMonth.compareToIgnoreCase("Nov") == 0) {
            return "11";
        }
        else if (shortMonth.compareToIgnoreCase("Dec") == 0) {
            return "12";
        } else {
            return "XX";
        }
    }

}
