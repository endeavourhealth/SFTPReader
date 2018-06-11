package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class BartsSftpFilenameParser extends SftpFilenameParser {

    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    //special constants for the 2.1 files, but we just use the first part of the file name for 2.2 files
    public static final String TYPE_2_1_SUSOPA = "2.1_SUSOPA";
    public static final String TYPE_2_1_SUSAEA = "2.1_SUSAEA";
    public static final String TYPE_2_1_SUSIP = "2.1_SUSIP";
    public static final String TYPE_2_1_TAILOPA = "2.1_TAILOPA";
    public static final String TYPE_2_1_TAILAEA = "2.1_TAILAEA";
    public static final String TYPE_2_1_TAILIP = "2.1_TAILIP";
    public static final String TYPE_2_1_PROC = "2.1_PROC";
    public static final String TYPE_2_1_DIAG = "2.1_DIAG";
    public static final String TYPE_2_1_BIRTH = "2.1_BIRTH";
    public static final String TYPE_2_1_PREG = "2.1_PREG";
    public static final String TYPE_2_1_PROB = "2.1_PROB";
    public static final String TYPE_2_2_SPFIT = "2.2_SPFIT"; //Surginet ???
    public static final String TYPE_2_2_CC = "2.2_CC"; //Critial Care
    public static final String TYPE_2_2_HDB = "2.2_HDB"; //Home Delivery and Birth
    public static final String TYPE_2_2_FAMILY_HISTORY = "2.2_FAMILY_HISTORY";

    private String fileTypeIdentifier;
    private LocalDate extractDate;
    private boolean isFileNeeded;

    public BartsSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        //batch the extract files by date
        return BATCH_IDENTIFIER_FORMAT.format(extractDate);
    }

    @Override
    public String generateFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

    @Override
    public boolean isFileNeeded(){
        return isFileNeeded;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        //changing to false for 2.2 so as we now should know what all the files mean
        return false;
        //return true;
    }

    public static LocalDate parseBatchIdentifier(String batchIdentifier) {
        return LocalDate.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
    }


    @Override
    protected void parseFilename() throws SftpFilenameParseException {

        //by default we want ALL files
        isFileNeeded = true;

        String fileName = this.remoteFile.getFilename();
        LocalDateTime lastModified = this.remoteFile.getLastModified();

        //we have a load of custom extract files that Barts have uploaded and are in the same S3 bucket as our normal files
        //for now, just ignore them with this
        if (fileName.equals("PI_CDE_PERSON_PATIENT.csv")
                || fileName.equals("PI_LKP_CDE_CODE_VALUE_REF.csv")
                || fileName.equals("PI_LKP_CDE_LOCATION_REF.zip")
                || fileName.equals("PI_LKP_CDE_LOCATION_REF_15FEB2018.zip")
                || fileName.equals("dump20180123.zip")
                || fileName.equals("PI_LKP_CDE_CODE_VALUE_REF.zip")
                || fileName.equals("PI_CDE_AE_ATTENDANCE.zip")
                || fileName.equals("PI_CDE_CLINICAL_EVENT.zip")

                || fileName.equals("PI_CDE_DIAGNOSIS.zip")
                || fileName.equals("PI_CDE_ENCOUNTER.zip")
                || fileName.equals("PI_CDE_ENCOUNTER_INFO.zip")
                || fileName.equals("PI_CDE_IP_EPISODE.zip")
                || fileName.equals("PI_CDE_IP_WARDSTAY.zip")
                || fileName.equals("PI_CDE_OP_ATTENDANCE.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_ADDRESS.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_ALIAS.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_INFO.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_NAME.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_PERSON_RELTN.zip")
                || fileName.equals("PI_CDE_PERSON_PATIENT_PHONE.zip") // PPPHO bulk 2018-03-09 500MB - TODO extract into S3 and add to extract
                || fileName.equals("PI_CDE_PROCEDURE.zip") //PROCE bulk file 2018-03-09 600MB - TODO extract into S3 and add to extract
                || fileName.equals("PI_LKP_CDE_ORG_REF_Dump20180611.zip") //ORGREF bulk 2018/06/11 - TODO extract into S3 and schedule in extract

                //these are artefacts of the copying and can be ignored
                || fileName.equals("PI_CDE_CLINICAL_EVENT.zip.filepart")
                || fileName.equals("PI_CDE_DIAGNOSIS.zip.filepart")
                || fileName.equals("PI_CDE_OP_ATTENDANCE.zip.filepart")
                || fileName.equals("PI_CDE_PERSON_PATIENT_PERSON_RELTN.zip.filepart")

                //these just one-off reference files not for processing
                || fileName.equals("V500_Event_Set_Code.xlsx")
                || fileName.equals("V500_event_code.xlsx")
                || fileName.equals("V500_other_table_contents.xlsx")) {
            isFileNeeded = false;
            return;
        }

        String baseName = FilenameUtils.getBaseName(fileName); //name without extension
        String[] toks = baseName.split("_");

        String tok1 = toks[0];
        if (tok1.equalsIgnoreCase("susopa")) {
            fileTypeIdentifier = TYPE_2_1_SUSOPA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("susaea")) {
            fileTypeIdentifier = TYPE_2_1_SUSAEA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("ip")) {
            fileTypeIdentifier = TYPE_2_1_SUSIP;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("tailopa")) {
            fileTypeIdentifier = TYPE_2_1_TAILOPA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("tailaea")) {
            fileTypeIdentifier = TYPE_2_1_TAILAEA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("tailip")) {
            fileTypeIdentifier = TYPE_2_1_TAILIP;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date
            //TODO - remove for AWS Live?
            isFileNeeded = false;

        } else if (tok1.equalsIgnoreCase("cc")) {
            fileTypeIdentifier = TYPE_2_2_CC;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("hdb")) {
            fileTypeIdentifier = TYPE_2_2_HDB;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("spfit")) {
            fileTypeIdentifier = TYPE_2_2_SPFIT;
            if (toks.length != 6) {
                throw new SftpFilenameParseException("Expecting six elements in filename [" + fileName + "]");
            }

            String tok6 = toks[5].substring(0, 8);
            extractDate = LocalDate.parse(tok6, DateTimeFormatter.ofPattern("yyyyMMdd"));

        } else if (tok1.equalsIgnoreCase("rnj")) {
            if (toks.length != 4) {
                throw new SftpFilenameParseException("Expecting four elements in filename starting rnj [" + fileName + "]");
            }

            String tok3 = toks[2];
            if (tok3.equalsIgnoreCase("proc")) {
                fileTypeIdentifier = TYPE_2_1_PROC;

            } else if (tok3.equalsIgnoreCase("prob")) {
                fileTypeIdentifier = TYPE_2_1_PROB;

            } else if (tok3.equalsIgnoreCase("diag")) {
                fileTypeIdentifier = TYPE_2_1_DIAG;

            } else {
                throw new SftpFilenameParseException("Unexpected third element " + tok3 + " after rnj in filename [" + fileName + "]");
            }

            String tok4 = toks[3].substring(0, 8);
            extractDate = LocalDate.parse(tok4, DateTimeFormatter.ofPattern("yyyyMMdd"));

        } else if (tok1.equalsIgnoreCase("GETL")) {
            if (toks.length != 6) {
                throw new SftpFilenameParseException("Expecting four elements in filename starting GETL [" + fileName + "]");
            }

            String tok3 = toks[2];
            if (tok3.equalsIgnoreCase("PREG")) {
                fileTypeIdentifier = TYPE_2_1_PREG;

            } else if (tok3.equalsIgnoreCase("BIRTH")) {
                fileTypeIdentifier = TYPE_2_1_BIRTH;

            } else {
                throw new SftpFilenameParseException("Unexpected third element " + tok3 + " after GETL in filename [" + fileName + "]");
            }

            String tok4 = toks[3].substring(0, 10);
            extractDate = LocalDate.parse(tok4, DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        } else if (tok1.equalsIgnoreCase("Fam")) {
            if (toks.length != 4) {
                throw new SftpFilenameParseException("Expecting four elements in filename starting Fam [" + fileName + "]");
            }

            String tok2 = toks[1];
            if (!tok2.equalsIgnoreCase("Hist")) {
                throw new SftpFilenameParseException("Expecting Hist elements in filename starting Fam [" + fileName + "]");
            }

            fileTypeIdentifier = TYPE_2_2_FAMILY_HISTORY;

            String tok3 = toks[2];
            extractDate = LocalDate.parse(tok3, DateTimeFormatter.ofPattern("yyyyMMdd"));

        } else {
            this.fileTypeIdentifier = tok1;

            if (toks.length < 4) {
                throw new SftpFilenameParseException("Expecting at least four elements in filename [" + fileName + "]");
            }

            //file date is either in the third or fourth element
            String tok3 = toks[2];
            if (tok3.equalsIgnoreCase("RNJ")) {
                String tok4 = toks[3];
                extractDate = LocalDate.parse(tok4, DateTimeFormatter.ofPattern("ddMMyyyy"));

            } else {
                extractDate = LocalDate.parse(tok3, DateTimeFormatter.ofPattern("ddMMyyyy"));

            }
        }
    }



    /*protected void parseFilename(String filename, LocalDateTime lastModified) throws SftpFilenameParseException {
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
            fileUniqueId = prefixZeros(filenamePart2.substring(filenamePart2.indexOf(".") + 1));

        } else if (filenamePart1.compareToIgnoreCase("susaea") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_SUSAEA;
            fileUniqueId = prefixZeros(filenamePart2.substring(filenamePart2.indexOf(".") + 1));
            batchGroup = FILE_TYPE_SUSAEA;

        } else if (filenamePart1.compareToIgnoreCase("tailopa") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_TAILOPA;
            fileUniqueId = prefixZeros(filenamePart2.substring(filenamePart2.indexOf(".") + 1));
            batchGroup = FILE_TYPE_SUSOPA;

        } else if (filenamePart1.compareToIgnoreCase("tailaea") == 0) {
            if (parts.length != 2)
                throw new SftpFilenameParseException("Barts batch filename could not be parsed");
            fileTypeIdentifier = FILE_TYPE_TAILAEA;
            fileUniqueId = prefixZeros(filenamePart2.substring(filenamePart2.indexOf(".") + 1));
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
                fileUniqueId = prefixZeros(filenamePart2.split("\\.")[1]);
                batchGroup = FILE_TYPE_SUSIP;
            } else {
                String filenamePart4 = parts[3];

                if (filenamePart1.compareToIgnoreCase("ip") == 0) {
                    if (parts.length != 4)
                        throw new SftpFilenameParseException("Barts batch filename could not be parsed");
                    fileTypeIdentifier = FILE_TYPE_SUSIP;
                    fileUniqueId = prefixZeros(filenamePart3);
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

    *//*
     *
     *//*
    private void parseBulkFilename(String filename, String pgpFileExtensionFilter) throws SftpFilenameParseException {
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

    *//*
     *
     *//*
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

    *//*
     *
     *//*
    private String prefixZeros(String inNumber) {
        int i = Integer.parseInt(inNumber);
        return String.format ("%010d", i);
    }*/

}
