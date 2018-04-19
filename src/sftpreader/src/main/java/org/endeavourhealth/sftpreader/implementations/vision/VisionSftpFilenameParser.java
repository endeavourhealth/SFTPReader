package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class VisionSftpFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private LocalDateTime extractDateTime;

    private String fileTypeIdentifier;
    private String fileContentTypeIdentifier;
    private String nacsCode;
    private String serviceIdentifier;
    private String formatIdentifier;
    private boolean isFileNeeded;

    private static final Logger LOG = LoggerFactory.getLogger(VisionSftpFilenameParser.class);

    /*public VisionSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }*/

    public VisionSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
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
        return fileTypeIdentifier + "_" + fileContentTypeIdentifier;
    }

    @Override
    public boolean isFileNeeded(){
        return isFileNeeded;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return false;
    }

    /*@Override
    public boolean requiresDecryption() {
        return false;
    }*/

    @Override
    protected void parseFilename() throws SftpFilenameParseException {

        //fileType_contentType_nacsCode-(serviceIdentifier)-formatVersion_2015-04-22-130917.zip
        //fileType = FULL / INCREMENTAL / ADDITIONAL
        //fileContentType = PATIENT / CLINICAL / ADMIN
        //nacsCode = Practice odsCode code
        //serviceIdentifier = Extract Service name
        //format version = V1.0.0
        //dateTime = YYYY-MM-DD-HHMMSS

        // get the three main parts of the filename
        String fileName = this.remoteFile.getFilename();
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
        String extractDateTime = fileFormatAndDateSectionParts[1].replace(".zip", "");;

        if (StringUtils.isEmpty(fileType))
            throw new SftpFilenameParseException("FileType is empty");

        this.fileTypeIdentifier = fileType;

        // Exclude FULL extracts as everything is supplied in INCREMENTAL extracts
        if (fileType.equals("FULL")) {
            this.isFileNeeded = false;
        } else {
            this.isFileNeeded = true;
        }

        LOG.debug("Filename: "+fileName+" needed? -> "+this.isFileNeeded());

        if (StringUtils.isEmpty(fileContentType))
            throw new SftpFilenameParseException("ContentType is empty");

        this.fileContentTypeIdentifier = fileContentType;

        if (StringUtils.isEmpty(nacsCode))
            throw new SftpFilenameParseException("NacsCode is empty");

        this.nacsCode = nacsCode;

        if (StringUtils.isEmpty(serviceIdentifier))
            throw new SftpFilenameParseException("ServiceIdentifier is empty");

        this.serviceIdentifier = serviceIdentifier;

        if (StringUtils.isEmpty(formatVersionPart))
            throw new SftpFilenameParseException("Format Version is empty");

        this.formatIdentifier = formatVersionPart;

        if (StringUtils.isEmpty(extractDateTime))
            throw new SftpFilenameParseException("Extract date/time is empty");

        extractDateTime = extractDateTime.replace("-","");
        this.extractDateTime = LocalDateTime.parse(extractDateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        //this will be a .zip for Vision extract files
        if (!StringUtils.endsWith(fileName, ".zip"))
            throw new SftpFilenameParseException("File does not end with .zip");
    }
}
