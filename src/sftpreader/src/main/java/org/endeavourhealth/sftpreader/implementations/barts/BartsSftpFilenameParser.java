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

    //special constants for the non-standard files, but we just use the first part of the file name for 2.2 files
    public static final String TYPE_2_1_SUSOPA = "SusOutpatient";
    public static final String TYPE_2_1_SUSAEA = "SusEmergency";
    public static final String TYPE_2_1_SUSIP = "SusInpatient";
    public static final String TYPE_2_1_TAILOPA = "SusOutpatientTail";
    public static final String TYPE_2_1_TAILAEA = "SusEmergencyTail";
    public static final String TYPE_2_1_TAILIP = "SusInpatientTail";
    public static final String TYPE_2_1_PROC = "Procedure";
    public static final String TYPE_2_1_DIAG = "Diagnosis";
    public static final String TYPE_2_1_BIRTH = "Birth";
    public static final String TYPE_2_1_PREG = "Pregnancy";
    public static final String TYPE_2_1_PROB = "Problem";
    public static final String TYPE_2_2_SPFIT = "SPFIT"; //Surginet ???
    public static final String TYPE_2_2_CC = "CriticalCare"; //Critial Care
    public static final String TYPE_2_2_HDB = "HomeDeliveryAndBirth"; //Home Delivery and Birth
    public static final String TYPE_2_2_FAMILY_HISTORY = "FamilyHistory";
    public static final String TYPE_EMERGENCY_CARE = "SusEmergencyCareDataSet";
    public static final String TYPE_EMERGENCY_CARE_TAILS = "SusEmergencyCareDataSetTail";
    public static final String TYPE_APPOINTMENT_SCHEDULING = "APPSL2";


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

        //we get partial files with extension filepart, which are artifacts of copying to our SFTP server, so ignore them
        String ext = FilenameUtils.getExtension(fileName);
        if (ext.equalsIgnoreCase("filepart")) {
            isFileNeeded = false;
            return;
        }

        //we have a load of custom extract files that Barts have uploaded and are in the same S3 bucket as our normal files
        //for now, just ignore them with this
        if (
                fileName.equals("PI_LKP_CDE_CODE_VALUE_REF.csv") //CVREF 2018/02/15 - dump with commas and extra cols - ignored as the below one replaces this
                || fileName.equals("PI_LKP_CDE_CODE_VALUE_REF.zip") //CVREF 2018/08/23 - copied as CVREF_80130_01122017_999999_1.txt
                || fileName.equals("PI_LKP_CDE_LOCATION_REF_15FEB2018.zip") //LOREF 2018/02/15 7MB - dump with with commas and extra cols - ignored as below one replaces this
                || fileName.equals("PI_LKP_CDE_LOCATION_REF.zip") //LOREF 2018/02/15 5MB - copied as LOREF_80130_RNJ_01122017_999999_1.csv
                || fileName.equals("PI_CDE_DIAGNOSIS.zip") //DIAGN 2018/03/09 980MB - copied as DIAGN_80130_RNJ_09032018_999999_2.TXT
                || fileName.equals("PI_CDE_PROCEDURE.zip") //PROCE bulk file 2018/03/09 600MB - copied as PROCE_80130_RNJ_09032018_999999_2.TXT
                || fileName.equals("PI_CDE_CLINICAL_EVENT.zip") //CLEVE 2018/03/12 - file was corrupt, so cannot be processed
                || fileName.equals("PI_CDE_ENCOUNTER.zip") //ENCNT 2018/03/09 3GB - copied as ENCNT_80130_RNJ_03122017_999999_1.TXT
                || fileName.equals("PI_CDE_ENCOUNTER_INFO.zip") //ENCINF 2018/03/09 6GB - copied as ENCINF_80130_RNJ_03122017_999999_3.TXT
                || fileName.equals("PI_CDE_OP_ATTENDANCE.zip") //OPATT 2018/03/09 2GB - copied as OPATT_80130_RNJ_03122017_999999_1.TXT
                || fileName.equals("PI_CDE_AE_ATTENDANCE.zip") //AEATT 2018/03/09 700MB - copied to AEATT_80130_RNJ_03122017_999999_1.TXT
                || fileName.equals("PI_CDE_IP_EPISODE.zip") //IPEPI 2018/03/09 200MB - copied to IPEPI_80130_RNJ_03122017_999999_1.TXT
                || fileName.equals("PI_CDE_IP_WARDSTAY.zip") //IPWDS 2018/03/09 300MB - copied to IPWDS_80130_RNJ_03122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_NAME.zip") //PPNAM 2018/03/09 400MB - copied to PPNAM_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_PHONE.zip") //PPPHO 2018/03/09 500MB - copied to PPPHO_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_ADDRESS.zip") //PPADD 2018/03/09 500MB - copied to PPADD_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_ALIAS.zip") //PPALI 2018/09/09 700MB - copied to PPALI_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_PERSON_RELTN.zip") //PPREL 2018/03/09 500MB - copied to PPREL_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT.csv") //PPATI 2018/02/15 280MB - dump has commas and extra cols - ignored as below one replaces this
                || fileName.equals("PI_CDE_PERSON_PATIENT.zip") //PPATI 2018/03/09 250MB - copied to PPATI_80130_RNJ_02122017_999999_1.TXT
                || fileName.equals("PI_CDE_PERSON_PATIENT_INFO.zip") //PPINF 2018/03/09 2GB - ignored, as we don't process this file
                || fileName.equals("dump20180123.zip") //dump of some old reference files - ignored, as we have never versions of the ones we use
                || fileName.equals("PI_LKP_CDE_ORG_REF_Dump20180611.zip") //ORGREF bulk 2018/06/11 - copied as ORGREF_80130_01122017_999999_1.TXT
                || fileName.equals("PI_CDE_CLINICAL_EVENT_20180613.zip") //CLEVE bulk 2018/06/13 - copied as CLEVE_80130_RNJ_13062018_999999_13.TXT
                || fileName.equals("PI_LKP_CDE_NOMENCLATURE_REF_20180615.csv") //NOMREF bulk 2018/06/15 - coped as NOMREF_80130_01122017_999999_1.TXT
                || fileName.equals("susecd_BH.19001") //no idea about this - asked Olu
                || fileName.equals("tailecd_DIS.19001") //no idea about this - asked Olu

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

        } else if (tok1.equalsIgnoreCase("susaea")) {
            fileTypeIdentifier = TYPE_2_1_SUSAEA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("ip")) {
            fileTypeIdentifier = TYPE_2_1_SUSIP;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("tailopa")) {
            fileTypeIdentifier = TYPE_2_1_TAILOPA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("tailaea")) {
            fileTypeIdentifier = TYPE_2_1_TAILAEA;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("tailip")) {
            fileTypeIdentifier = TYPE_2_1_TAILIP;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("cc")) {
            fileTypeIdentifier = TYPE_2_2_CC;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("hdb")) {
            fileTypeIdentifier = TYPE_2_2_HDB;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("susecd")) {
            fileTypeIdentifier = TYPE_EMERGENCY_CARE;
            extractDate = lastModified.toLocalDate(); //filename doesn't have the date, so use the modified date

        } else if (tok1.equalsIgnoreCase("tailecd")) {
            fileTypeIdentifier = TYPE_EMERGENCY_CARE_TAILS;
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
            if (toks.length == 6) {
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

            } else if (toks.length == 7) {
                String tok2 = toks[1];
                if (tok2.equals("APPSL2")) {
                    fileTypeIdentifier = TYPE_APPOINTMENT_SCHEDULING;

                    String tok5 = toks[4];
                    extractDate = LocalDate.parse(tok5, DateTimeFormatter.ofPattern("ddMMyyyy"));
                } else {
                    throw new SftpFilenameParseException("Unexpected second element " + tok2 + " after GETL in filename [" + fileName + "]");
                }

            } else {
                throw new SftpFilenameParseException("Unexpected number of elements after GETL in filename [" + fileName + "]");
            }

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


}
