package org.endeavourhealth.sftpreader.utilities;

import com.jcraft.jsch.*;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnectionException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class Connection {
    protected static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Connection.class);
    private ConnectionDetails connectionDetails;

    public Connection(ConnectionDetails connectionDetails) {
        this.connectionDetails = connectionDetails;
    }

    public abstract void open() throws Exception;

    public abstract List<RemoteFile> getFileList(String remotePath) throws Exception;

    // Return a single remote file
    public abstract InputStream getFile(String remotePath) throws Exception;

    // Delete remote file
    public abstract void deleteFile(String remotePath) throws Exception;

    // Change remote default directory
    //public abstract void cd(String remotePath) throws Exception;

    // Upload/save local file to remote
    public abstract void put(String localPath, String destinationPath) throws Exception;

    // Create new remote directory
    public abstract void mkDir(String path) throws Exception;

    // Close external connection
    public abstract void close();

    public ConnectionDetails getConnectionDetails() {
        return this.connectionDetails;
    }
}
