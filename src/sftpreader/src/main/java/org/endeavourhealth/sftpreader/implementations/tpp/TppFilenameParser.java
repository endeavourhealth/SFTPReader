package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
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
import java.time.format.DateTimeParseException;
import java.util.UUID;

public class TppFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename and sorted
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");
    private static final DateTimeFormatter SOURCE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm");

    private String fileTypeIdentifier;
    private LocalDateTime extractDateTime;

    public TppFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
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

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        //file type is just the base file name (e.g. SRManifest) minus the "SR"
        String fileName = this.remoteFile.getFilename();
        fileName = FilenameUtils.getBaseName(fileName);

        if (!fileName.startsWith("SR")) {
            throw new SftpFilenameParseException("File " + fileName + " doesn't start with SR");
        }

        this.fileTypeIdentifier = fileName.substring(2); //get rid of "SR"

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

    public static LocalDateTime parseBatchIdentifier(String batchIdentifier) {
        return LocalDateTime.parse(batchIdentifier, BATCH_IDENTIFIER_FORMAT);
    }

}
