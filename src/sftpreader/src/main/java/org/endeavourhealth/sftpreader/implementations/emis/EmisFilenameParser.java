package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.implementations.SftpFilenameParser;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbConfigurationKvp;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class EmisFilenameParser extends SftpFilenameParser {
                                                                        // same as ISO pattern but switch : for . so can be used as filename and sorted
    private static final DateTimeFormatter BATCH_IDENTIFIER_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH'.'mm'.'ss");

    private ProcessingIdSet processingIds;
    private String fileTypeIdentifier;
    private LocalDateTime extractDateTime;

    /*public EmisSftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        super(filename, dbConfiguration, fileExtension);
    }*/

    public EmisFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {
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
        return true;
    }

    @Override
    public boolean ignoreUnknownFileTypes() {
        return false;
    }

    /*@Override
    public boolean requiresDecryption() {
        return true;
    }*/

    @Override
    protected void parseFilename(boolean isRawFile) throws SftpFilenameParseException {

        String fileName = this.remoteFile.getFilename();
        String[] parts = fileName.split("_");

        if (parts.length != 5) {
            throw new SftpFilenameParseException("Emis batch filename could not be parsed");
        }

        String processingIdPart = parts[0];
        String schemaNamePart = parts[1];
        String tableNamePart = parts[2];
        String extractDateTimePart = parts[3];
        String sharingAgreementGuidWithFileExtensionPart = parts[4];

        this.processingIds = ProcessingIdSet.parseBatchIdentifier(processingIdPart);

        if (StringUtils.isEmpty(schemaNamePart)) {
            throw new SftpFilenameParseException("Schema name is empty");
        }

        if (StringUtils.isEmpty(tableNamePart)) {
            throw new SftpFilenameParseException("Table name is empty");
        }

        if (StringUtils.isEmpty(extractDateTimePart)) {
            throw new SftpFilenameParseException("Extract date/time is empty");
        }

        this.fileTypeIdentifier = schemaNamePart + "_" + tableNamePart;

        this.extractDateTime = LocalDateTime.parse(extractDateTimePart, DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
    }


    public ProcessingIdSet getProcessingIds() {
        return this.processingIds;
    }

    public LocalDateTime getExtractDateTime() {
        return extractDateTime;
    }

    public static String getDecryptedFileName(BatchFile batchFile, DbConfiguration dbConfiguration) {
        return getDecryptedFileName(batchFile.getFilename(), dbConfiguration);
    }

    public static String getDecryptedFileName(String encryptedFilename, DbConfiguration dbConfiguration) {
        String extension = dbConfiguration.getPgpFileExtensionFilter();
        return StringUtils.removeEnd(encryptedFilename, extension);
    }
}
