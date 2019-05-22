package org.endeavourhealth.sftpreader.implementations;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public abstract class SftpFilenameParser {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpFilenameParser.class);

    private boolean isRawFile;
    protected DbConfiguration dbConfiguration;
    protected RemoteFile remoteFile;

    private boolean isFilenameValid = false;

    public SftpFilenameParser(boolean isRawFile, RemoteFile remoteFile, DbConfiguration dbConfiguration) {

        Validate.notNull(dbConfiguration, "dbConfiguration is null");

        this.isRawFile = isRawFile;
        this.remoteFile = remoteFile;
        this.dbConfiguration = dbConfiguration;

        try {
            //parseFilename(filename, this.fileExtension);
            parseFilename(isRawFile);

            if (this.isRawFile
                && this.isFileNeeded()
                && !dbConfiguration.getInterfaceFileTypes().contains(generateFileTypeIdentifier())) {

                throw new SftpFilenameParseException("File type " + generateFileTypeIdentifier() + " not recognised");
            }

            this.isFilenameValid = true;
        } catch (Exception e) {
            this.isFilenameValid = false;
            LOG.error("Error parsing filename: " + remoteFile.getFullPath(), e);
        }
    }

    protected abstract void parseFilename(boolean isRawFile) throws SftpFilenameParseException;
    public abstract String generateBatchIdentifier();
    public abstract String generateFileTypeIdentifier();
    public abstract boolean isFileNeeded();
    public abstract boolean ignoreUnknownFileTypes();
    //public abstract boolean requiresDecryption();

    public boolean isFilenameValid() {
        return this.isFilenameValid;
    }

}
