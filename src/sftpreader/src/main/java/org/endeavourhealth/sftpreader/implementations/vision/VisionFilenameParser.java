package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class VisionFilenameParser extends SftpFilenameParser {
    private static final Logger LOG = LoggerFactory.getLogger(VisionFilenameParser.class);
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private LocalDateTime extractDateTime;
    private String fileTypeIdentifier;
    private boolean isFileNeeded;

/*    private String fileTypeIdentifier;
    private String fileContentTypeIdentifier;
    private String nacsCode;
    private String serviceIdentifier;
    private String formatIdentifier;*/


    public VisionFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(isRawFile, remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return this.extractDateTime.format(BATCH_IDENTIFIER_FORMAT);
    }

    public static LocalDateTime parseBatchIdentifier(String batchIdentifier) {
        return LocalDateTime.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
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
        return true;  //to ignore the .csv as part of test
    }

    /*@Override
    public boolean requiresDecryption() {
        return false;
    }*/

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        if (isRawFile) {
            parseFilenameRaw();
        } else {
            parseFilenameUnzipped();
        }
    }

    private void parseFilenameUnzipped() throws SftpFilenameParseException {

        String fileName = this.remoteFile.getFilename();

        //this will be a csv for unzipped files
        String ext = FilenameUtils.getExtension(fileName);
        if (!ext.equalsIgnoreCase("csv")) {
            throw new SftpFilenameParseException("Expecting csv file");
        }

        //e.g. ADDITIONAL_F86007_active_user_data_extract-2018-08-17-000500.csv
        //     INCREMENTAL_F86007_journal_data_extract-2018-08-17-000500.csv
        String baseName = FilenameUtils.getBaseName(fileName);
        String[] toks = baseName.split("-|_"); //split by hyphen OR underscore
        List<String> typeToksList = new ArrayList<>();
        List<String> dateToksList = new ArrayList<>();

        for (int i=0; i<toks.length; i++) {
            String tok = toks[i];

            if (i>=2 && i<toks.length-4) {
                typeToksList.add(tok);
            }

            if (i>=toks.length-4) {
                dateToksList.add(tok);
            }
        }

        this.fileTypeIdentifier = String.join("_", typeToksList);
        this.isFileNeeded = true;

        String dateTimeStr = String.join("", dateToksList);
        this.extractDateTime = LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }

    private void parseFilenameRaw() throws SftpFilenameParseException {

        //fileType_contentType_nacsCode-(serviceIdentifier)-formatVersion_2015-04-22-130917.zip
        //fileType = FULL / INCREMENTAL / ADDITIONAL
        //fileContentType = PATIENT / CLINICAL / ADMIN
        //nacsCode = Practice odsCode code
        //serviceIdentifier = Extract Service name
        //format version = V1.0.0
        //dateTime = YYYY-MM-DD-HHMMSS

        // get the three main parts of the filename
        String fileName = this.remoteFile.getFilename();

        //this will be a .zip for raw Vision extract files and csv for unzipped ones
        String ext = FilenameUtils.getExtension(fileName);
        if (!ext.equalsIgnoreCase("zip")) {
            throw new SftpFilenameParseException("Filename does not end with .zip");
        }

        String[] parts = fileName.split("-", 3);
        if (parts.length != 3)
            throw new SftpFilenameParseException("Vision batch filename could not be parsed");

        String fileTypeParts [] = parts[0].split("_");
        String serviceIdentifier = parts[1].replace("(","").replace(")","");;
        String fileFormatAndDateSectionParts [] = parts[2].split("_");
        String fileType = fileTypeParts [0];
        String fileContentType = fileTypeParts [1];
        String nacsCode = fileTypeParts [2];

        String formatVersionPart = fileFormatAndDateSectionParts[0];
        String extractDateTimeStr = fileFormatAndDateSectionParts[1].replace(".zip", "");;

        if (StringUtils.isEmpty(fileType)) {
            throw new SftpFilenameParseException("FileType is empty");
        }

        if (StringUtils.isEmpty(fileContentType)) {
            throw new SftpFilenameParseException("ContentType is empty");
        }

        if (StringUtils.isEmpty(nacsCode)) {
            throw new SftpFilenameParseException("NacsCode is empty");
        }

        if (StringUtils.isEmpty(serviceIdentifier)) {
            throw new SftpFilenameParseException("ServiceIdentifier is empty");
        }

        if (StringUtils.isEmpty(formatVersionPart)) {
            throw new SftpFilenameParseException("Format Version is empty");
        }

        if (StringUtils.isEmpty(extractDateTimeStr)) {
            throw new SftpFilenameParseException("Extract date/time is empty");
        }

        // Exclude FULL extracts as everything is supplied in INCREMENTAL extracts
        if (fileType.equals("FULL")) {
            this.isFileNeeded = false;
        } else {
            this.isFileNeeded = true;
        }

        this.fileTypeIdentifier = fileType + "_" + fileContentType;

        extractDateTimeStr = extractDateTimeStr.replace("-","");
        this.extractDateTime = LocalDateTime.parse(extractDateTimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }


    @Override
    public Date getExtractDate() {
        return java.util.Date.from(extractDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant());
    }
}
