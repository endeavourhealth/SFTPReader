package org.endeavourhealth.sftpreader.implementations;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.slf4j.LoggerFactory;

public abstract class SftpFilenameParser {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpFilenameParser.class);

    private boolean isFilenameValid = false;
    private boolean isFileNeeded = true;
    private boolean ignoreUnknownFileTypes = false;
    protected DbConfiguration dbConfiguration;
    private String fileExtension = null;

    public SftpFilenameParser(String filename, DbConfiguration dbConfiguration) {
        this(filename, dbConfiguration, dbConfiguration.getPgpFileExtensionFilter());
    }

    public SftpFilenameParser(String filename, DbConfiguration dbConfiguration, String fileExtension) {
        Validate.notNull(dbConfiguration, "dbConfiguration is null");

        this.dbConfiguration = dbConfiguration;
        this.fileExtension = fileExtension;

        try {
            parseFilename(filename, this.fileExtension);

            this.isFileNeeded = isFileNeeded();

            this.ignoreUnknownFileTypes = ignoreUnknownFileTypes();

            if (!dbConfiguration.getInterfaceFileTypes().contains(generateFileTypeIdentifier()))
                throw new SftpFilenameParseException("File type " + generateFileTypeIdentifier() + " not recognised");

            this.isFilenameValid = true;
        } catch (Exception e) {
            this.isFilenameValid = false;
            LOG.error("Error parsing filename: " + filename, e);
        }
    }

    protected abstract void parseFilename(String filename, String pgpFileExtensionFilter) throws SftpFilenameParseException;
    protected abstract String generateBatchIdentifier();
    protected abstract String generateFileTypeIdentifier();
    protected abstract boolean isFileNeeded();
    public abstract boolean ignoreUnknownFileTypes();


    public boolean isFilenameValid() {
        return this.isFilenameValid;
    }

    public boolean isFileNameNeeded() {
        return this.isFileNeeded;
    }



    public String getBatchIdentifier() {
        if (!isFilenameValid)
            return "UNKNOWN";

        return generateBatchIdentifier();
    }

    public String getFileTypeIdentifier() {
        if (!isFilenameValid)
            return "UNKNOWN";

        return generateFileTypeIdentifier();
    }

    public String getFileExtension() {
        return this.fileExtension;
    }
}
