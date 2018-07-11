package org.endeavourhealth.sftpreader.implementations.homerton;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class HomertonSftpFilenameParser extends SftpFilenameParser {

    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static final String FILE_TYPE_ALLERGY = "ALLERGY";
    public static final String FILE_TYPE_CLINEVENT = "CLINEVENT";
    public static final String FILE_TYPE_PATIENT = "PATIENT";
    public static final String FILE_TYPE_PROCEDURE = "PROCEDURE";
    public static final String FILE_TYPE_DIAGNOSIS = "DIAGNOSIS";
    public static final String FILE_TYPE_LOCATION = "LOCATION";
    public static final String FILE_TYPE_LOCATION_GROUP = "LOCATIONGROUP";

    public static final String FILE_TYPE_ENCOUNTER_ALIAS = "ENCALI";
    public static final String FILE_TYPE_ENCOUNTER_PRSNL_RELTN = "ENCPRSNLRELTN";
    public static final String FILE_TYPE_ENCOUNTER_SLICE = "ENCNTRSLICE";

    public static final String FILE_TYPE_PERSON_ALIAS = "PERSONALIAS";
    public static final String FILE_TYPE_PROBLEM = "PROBLEM";

    public static final String FILE_TYPE_ENCOUNTER = "ENCOUNTER";
    public static final String FILE_TYPE_CODES = "CODES";


    private String fileTypeIdentifier;
    private LocalDate extractDate;

    public HomertonSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return this.extractDate.format(BATCH_IDENTIFIER_FORMAT);
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

    public static LocalDate parseBatchIdentifier(String batchIdentifier) {
        return LocalDate.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
    }

    @Override
    protected void parseFilename() throws SftpFilenameParseException {

        //filename is of format:  FILETYPE_EXTRACTEFFECTIVEDATE_EXTRACTRUNDATE
        //use the EXTRACTEFFECTIVEDATE for batch yyyyMMdd as there will be only one per day based on the stored procedure

        String fileName = this.remoteFile.getFilename();

        //this will be a .csv extract file
        if (!StringUtils.endsWith(fileName, ".csv"))
            throw new SftpFilenameParseException("Filename does not end with .csv");

        String[] parts = fileName.split("_");

        if (parts.length < 2)
            throw new SftpFilenameParseException("Homerton batch filename could not be parsed");

        String filenamePart1 = parts[0];
        String filenamePart2 = parts[1];

        extractDate = LocalDate.parse(filenamePart2, DateTimeFormatter.ofPattern("yyyyMMdd"));

        if (filenamePart1.compareToIgnoreCase("ALLERGY") == 0) {
            fileTypeIdentifier = FILE_TYPE_ALLERGY;
        } else if (filenamePart1.compareToIgnoreCase("PATIENT") == 0) {
            fileTypeIdentifier = FILE_TYPE_PATIENT;
        } else if (filenamePart1.compareToIgnoreCase("CLINEVENT") == 0) {
            fileTypeIdentifier = FILE_TYPE_CLINEVENT;
        } else if (filenamePart1.compareToIgnoreCase("CODES") == 0) {
            fileTypeIdentifier = FILE_TYPE_CODES;
        } else if (filenamePart1.compareToIgnoreCase("DIAGNOSIS") == 0) {
            fileTypeIdentifier = FILE_TYPE_DIAGNOSIS;
        } else if (filenamePart1.compareToIgnoreCase("ENCOUNTER") == 0) {
            fileTypeIdentifier = FILE_TYPE_ENCOUNTER;
        } else if (filenamePart1.compareToIgnoreCase("ENCALI") == 0) {
            fileTypeIdentifier = FILE_TYPE_ENCOUNTER_ALIAS;
        } else if (filenamePart1.compareToIgnoreCase("ENCPRSNLRELTN") == 0) {
            fileTypeIdentifier = FILE_TYPE_ENCOUNTER_PRSNL_RELTN;
        } else if (filenamePart1.compareToIgnoreCase("ENCNTRSLICE") == 0) {
            fileTypeIdentifier = FILE_TYPE_ENCOUNTER_SLICE;
        } else if (filenamePart1.compareToIgnoreCase("LOCATION") == 0) {
            fileTypeIdentifier = FILE_TYPE_LOCATION;
        } else if (filenamePart1.compareToIgnoreCase("LOCATIONGROUP") == 0) {
            fileTypeIdentifier = FILE_TYPE_LOCATION_GROUP;
        } else if (filenamePart1.compareToIgnoreCase("PERSONALIAS") == 0) {
            fileTypeIdentifier = FILE_TYPE_PERSON_ALIAS;
        } else if (filenamePart1.compareToIgnoreCase("PROBLEM") == 0) {
            fileTypeIdentifier = FILE_TYPE_PROBLEM;
        } else if (filenamePart1.compareToIgnoreCase("PROCEDURE") == 0) {
            fileTypeIdentifier = FILE_TYPE_PROCEDURE;
        } else {
            throw new SftpFilenameParseException("Homerton batch filename could not be parsed");
        }
    }


}
