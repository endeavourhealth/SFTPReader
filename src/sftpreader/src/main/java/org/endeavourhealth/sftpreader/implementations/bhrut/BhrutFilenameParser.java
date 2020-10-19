package org.endeavourhealth.sftpreader.implementations.bhrut;

import org.apache.commons.io.FilenameUtils;
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

public class BhrutFilenameParser extends SftpFilenameParser {
    private static final Logger LOG = LoggerFactory.getLogger(BhrutFilenameParser.class);
                                                                        // same as ISO pattern but switch : for . so can be used as filename
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private LocalDateTime extractDateTime;
    private String fileTypeIdentifier;
    private boolean isFileNeeded;

    public BhrutFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
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

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        String fileName = this.remoteFile.getFilename();

        //this will be a csv file
        String ext = FilenameUtils.getExtension(fileName);
        if (!ext.equalsIgnoreCase("csv")) {
            throw new SftpFilenameParseException("Unexpected file extension ("+ext+") for filename: "+fileName);
        }

        //check for rogue characters in filename
        String baseName = FilenameUtils.getBaseName(fileName);
        String fullPath = this.remoteFile.getFullPath();
        if (baseName.contains("%") || fullPath.contains("/\\")) {

            throw new SftpFilenameParseException("Unexpected filename format found: "+fullPath);
        }

        //filename format from v1.3 specification
        //e.g. BHRUT_1_PATIENT_ALERTS_DW_20200526221214.csv or BHRUT_1_PMI_DW_20200526221214.csv

        //split the filename into parts using the underscore
        String[] tokens = baseName.split("_");

        if (tokens.length == 5) {

            this.fileTypeIdentifier = tokens[2];
            String dateTimeStr = tokens[4];
            this.extractDateTime = LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        } else if (tokens.length == 6) {

            this.fileTypeIdentifier = tokens[2] + "_" + tokens[3];
            String dateTimeStr = tokens[5];
            this.extractDateTime = LocalDateTime.parse(dateTimeStr, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        } else {

            throw new SftpFilenameParseException("Unexpected number of tokens ("+tokens.length+") in filename: "+fileName);
        }

        this.isFileNeeded = true;

        //Any files created before 12th July 2020 are invalid, i.e. when the first valid bulk was created
        LocalDateTime cutoff = LocalDateTime.parse("12072020", DateTimeFormatter.ofPattern("ddMMyyyy"));
        if (this.extractDateTime.isBefore(cutoff)) {
            this.isFileNeeded = false;
        }
    }

    @Override
    public Date getExtractDate() {
        return Date.from(extractDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant());
    }
}