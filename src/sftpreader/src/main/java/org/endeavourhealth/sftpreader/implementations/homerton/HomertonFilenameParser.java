package org.endeavourhealth.sftpreader.implementations.homerton;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;

public class HomertonFilenameParser extends SftpFilenameParser {

    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");
    private static final DateTimeFormatter SOURCE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");

    private String fileTypeIdentifier;
    private LocalDateTime extractDateTime;

    public HomertonFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(isRawFile, remoteFile, dbConfiguration);
    }

    @Override
    public String generateBatchIdentifier() {
        return this.extractDateTime.format(BATCH_IDENTIFIER_FORMAT);
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

    public static LocalDateTime parseBatchIdentifier(String batchIdentifier) {
        return LocalDateTime.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
    }

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        //filename is of format:
        //PH_D_Person_Demographics.csv  and PH_D_Person_Demographics_Deleted.csv
        String fileName = this.remoteFile.getFilename();

        //this will be a .csv extract file
        if (!FilenameUtils.getExtension(fileName).equalsIgnoreCase("csv"))
            throw new SftpFilenameParseException("Filename does not end with .csv: "+fileName);

        //filename without extension
        String fileNameNoExt = FilenameUtils.getBaseName(fileName);

        //has to be at least 3 parts and longer that 5 characters to be valid
        String[] parts = fileNameNoExt.split("_");

        if (parts.length < 3 || fileNameNoExt.length() < 6 )
            throw new SftpFilenameParseException("Unexpected Homerton batch filename format: "+fileName);

        //strip out the prefixes such as PH_D_ to get a file type of Person_Demographics for example
        this.fileTypeIdentifier = fileNameNoExt.substring(5);

        //batch identifier is the datetime, which is found in the directory structure the files are in
        String filePath = this.remoteFile.getFullPath();
        File f = new File(filePath);

        //depending when this parser is called (either pre-splitting or when creating the JSON exchange payload),
        //the exchange date is at different points in the path and with different format, so we need to work up the path testing each level
        while (true) {

            f = f.getParentFile();
            if (f == null) {
                throw new RuntimeException("Failed to find parent directory with date time as name");
            }

            try {
                String dateDir = f.getName();
                if (isRawFile) {
                    this.extractDateTime = LocalDateTime.parse(dateDir, SOURCE_DATE_FORMAT);
                } else {
                    this.extractDateTime = LocalDateTime.parse(dateDir, BATCH_IDENTIFIER_FORMAT);
                }
                break;

            } catch (DateTimeParseException ex) {
                //let the loop continue
            }
        }
    }

    @Override
    public Date getExtractDate() {
        return java.util.Date.from(extractDateTime
                .atZone(ZoneId.systemDefault())
                .toInstant());
    }
}
