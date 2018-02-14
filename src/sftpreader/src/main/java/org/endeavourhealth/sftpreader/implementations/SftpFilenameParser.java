package org.endeavourhealth.sftpreader.implementations;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public abstract class SftpFilenameParser {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpFilenameParser.class);

    private boolean isFilenameValid = false;
    protected DbConfiguration dbConfiguration;
    //private String fileExtension = null;

    public SftpFilenameParser(String filename, LocalDateTime lastModified, DbConfiguration dbConfiguration) {

        Validate.notNull(dbConfiguration, "dbConfiguration is null");

        this.dbConfiguration = dbConfiguration;

        try {
            //parseFilename(filename, this.fileExtension);
            parseFilename(filename, lastModified);

            if (!dbConfiguration.getInterfaceFileTypes().contains(generateFileTypeIdentifier())) {
                throw new SftpFilenameParseException("File type " + generateFileTypeIdentifier() + " not recognised");
            }

            this.isFilenameValid = true;
        } catch (Exception e) {
            this.isFilenameValid = false;
            LOG.error("Error parsing filename: " + filename, e);
        }
    }

    protected abstract void parseFilename(String filename, LocalDateTime lastModified) throws SftpFilenameParseException;
    public abstract String generateBatchIdentifier();
    public abstract String generateFileTypeIdentifier();
    public abstract boolean isFileNeeded();
    public abstract boolean ignoreUnknownFileTypes();
    public abstract boolean requiresDecryption();

    public boolean isFilenameValid() {
        return this.isFilenameValid;
    }

    /*public String getBatchIdentifier() {
        if (!isFilenameValid)
            return "UNKNOWN";

        return generateBatchIdentifier();
    }

    public String getFileTypeIdentifier() {
        if (!isFilenameValid)
            return "UNKNOWN";

        return generateFileTypeIdentifier();
    }*/

    /*public String getFileExtension() {
        return this.fileExtension;
    }*/
}
