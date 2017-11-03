package org.endeavourhealth.sftpreader.implementations.vision;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;

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

    public VisionSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }

    public VisionSftpFilenameParser(String filename, DbConfiguration dbConfiguration) {
        super(filename, dbConfiguration);
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
    protected void parseFilename(String filename, String pgpFileExtensionFilter) throws SftpFilenameParseException {

        //fileType_contentType_nacsCode_(serviceIdentifier)_formatVersion-dateTime.zip
        //fileType = FULL / INCREMENTAL / ADDITIONAL
        //fileContentType = PATIENT / CLINICAL / ADMIN
        //nacsCode = Practice odsCode code
        //serviceIdentifier = Extract Service name
        //format version = v1.0.0
        //dateTime = YYYY-MM-DD_HHMMSS

        String[] parts = filename.split("_");

        if (parts.length != 6)
            throw new SftpFilenameParseException("Vision batch filename could not be parsed");

        String fileType = parts[0];
        String fileContentType = parts[1];
        String nacsCode = parts[2];
        String serviceIdentifier = parts[3];
        String extractFormatAndDate = parts[4];
        String [] endParts = extractFormatAndDate.split("-");
        String formatVersionPart = endParts[0];
        String extractDateYearPart = endParts[1];
        String extractDateMonthPart = endParts[2];
        String extractDateDayPart = endParts[3];
        String extractDateTimePart = parts[5].replace(".zip", "");
        String extractDateTime = extractDateYearPart.concat(extractDateMonthPart).concat(extractDateDayPart).concat(extractDateTimePart);

        if (StringUtils.isEmpty(fileType))
            throw new SftpFilenameParseException("FileType is empty");

        this.fileTypeIdentifier = fileType;

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

        if (StringUtils.isEmpty(extractDateTimePart))
            throw new SftpFilenameParseException("Extract date/time is empty");

        this.extractDateTime = LocalDateTime.parse(extractDateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        //this will be a .zip for Vision extract files
        if (!StringUtils.endsWith(filename, ".zip"))
            throw new SftpFilenameParseException("File does not end with .zip");
    }
}
