package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbConfigurationKvp;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class TppSftpFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename and sorted
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private String fileTypeIdentifier;
    private LocalDateTime extractDateTime;

    public TppSftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {
        super(remoteFile, dbConfiguration);
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

    @Override
    protected void parseFilename() throws SftpFilenameParseException {

        //file type is just the base file name (e.g. SRManifest) minus the "SR"
        String fileName = this.remoteFile.getFilename();
        fileName = FilenameUtils.getBaseName(fileName);

        if (!fileName.startsWith("SR")) {
            throw new SftpFilenameParseException("File " + fileName + " doesn't start with SR");
        }

        this.fileTypeIdentifier = fileName.substring(2);

        //batch identifier is the datetime, but we need to convert the format so it's consistent with Emis etc.
        String filePath = this.remoteFile.getFullPath();
        filePath = FilenameUtils.getPath(filePath);
        String dateDir = new File(filePath).getName();

        this.extractDateTime = LocalDateTime.parse(dateDir, DateTimeFormatter.ofPattern("yyyyMMdd_HHmm"));
    }

    public static LocalDateTime parseBatchIdentifier(String batchIdentifier) {
        return LocalDateTime.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
    }

}
