package org.endeavourhealth.sftpreader.implementations;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public abstract class SftpFilenameParser {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpFilenameParser.class);

    protected DbConfiguration dbConfiguration;
    protected RemoteFile remoteFile;

    private boolean isFilenameValid = false;

    public SftpFilenameParser(RemoteFile remoteFile, DbConfiguration dbConfiguration) {

        Validate.notNull(dbConfiguration, "dbConfiguration is null");

        this.remoteFile = remoteFile;
        this.dbConfiguration = dbConfiguration;

        try {
            //parseFilename(filename, this.fileExtension);
            parseFilename();

            if (this.isFileNeeded() && !dbConfiguration.getInterfaceFileTypes().contains(generateFileTypeIdentifier())) {

                String typeId = generateFileTypeIdentifier();
                LOG.error("file type ID [" + typeId + "]");
                LOG.error("file types:");
                for (String type: dbConfiguration.getInterfaceFileTypes()) {
                    LOG.error("type [" + type + "] = " + (typeId.equals(type)) + ", no case = " + (typeId.equalsIgnoreCase(type)));
                }

                throw new SftpFilenameParseException("File type " + generateFileTypeIdentifier() + " not recognised");
            }

            this.isFilenameValid = true;
        } catch (Exception e) {
            this.isFilenameValid = false;
            LOG.error("Error parsing filename: " + remoteFile.getFullPath(), e);
        }
    }

    protected abstract void parseFilename() throws SftpFilenameParseException;
    public abstract String generateBatchIdentifier();
    public abstract String generateFileTypeIdentifier();
    public abstract boolean isFileNeeded();
    public abstract boolean ignoreUnknownFileTypes();
    //public abstract boolean requiresDecryption();

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
