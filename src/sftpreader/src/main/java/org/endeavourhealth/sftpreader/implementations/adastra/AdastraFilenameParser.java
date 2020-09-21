package org.endeavourhealth.sftpreader.implementations.adastra;

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
import java.util.Date;

public class AdastraFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private LocalDateTime extractDateTime;
    private String fileContentTypeIdentifier;
    private String nacsCode;
    private boolean isFileNeeded;

    private static final Logger LOG = LoggerFactory.getLogger(AdastraFilenameParser.class);


    public AdastraFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
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
        return fileContentTypeIdentifier;
    }

    @Override
    public boolean isFileNeeded(){
        return isFileNeeded;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return true;  //to ignore the .csv as part of test
    }

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        //File format:  Adastra_nacsCode_fileContentType_yyyyMMddHHmmss.csv
        //nacsCode = OOH organisation odsCode code
        //fileContentType = PATIENT, CASE, CONSULTATION etc.
        //dateTime = yyyyMMddHHmmss

        // get the four main parts of the filename
        String fileName = this.remoteFile.getFilename();

        //ignore garbage SFTP temp etc. files
        if (fileName.equalsIgnoreCase("motd.legal-displayed")) { //message of the day file
            isFileNeeded = false;
            return;
        }

        //this will be a .csv extract file
        if (!StringUtils.endsWith(fileName, ".csv"))
            throw new SftpFilenameParseException("Filename does not end with .csv");

        String[] parts = fileName.split("_");
        if (parts.length != 4)
            throw new SftpFilenameParseException("Adastra batch filename could not be parsed.  Expecting 4 parts");

        if (!parts[0].equalsIgnoreCase("Adastra")) {
            throw new SftpFilenameParseException("File " + fileName + " doesn't start with Adastra");
        }

        String nacsCode = parts [1];
        if (StringUtils.isEmpty(nacsCode))
            throw new SftpFilenameParseException("NacsCode is empty");
        this.nacsCode = nacsCode;

        String fileContentType = parts [2];
        if (StringUtils.isEmpty(fileContentType))
            throw new SftpFilenameParseException("fileContentType is empty");
        this.fileContentTypeIdentifier = fileContentType;


        String extractDateTime = parts[3].replace(".csv", "");;
        if (StringUtils.isEmpty(extractDateTime))
            throw new SftpFilenameParseException("Extract date/time is empty");

        this.extractDateTime = LocalDateTime.parse(extractDateTime, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        this.isFileNeeded = true;

        // Removed this check now we have agreed to move forward with v2 format files and previous fixed data replaced
        //if it is City and Hackney CHUHSE OOH, we have received rubbish data between two particular dates which we want to ignore
//        if (this.nacsCode.equalsIgnoreCase("Y04474")) {
//
//            LocalDateTime startDate = LocalDateTime.parse("20180516181057", DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
//            LocalDateTime endDate = LocalDateTime.parse("20180719090925", DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
//
//            //if the extract date equals or falls between the two known erroneous dates, ignore the file
//            if ((this.extractDateTime.isEqual(startDate) || this.extractDateTime.isEqual(endDate)) ||
//                    (this.extractDateTime.isAfter(startDate) && this.extractDateTime.isBefore(endDate))) {
//                this.isFileNeeded = false;
//            }
//        }
    }

    @Override
    public Date getExtractDate() {
        return java.util.Date.from(extractDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant());
    }
}
