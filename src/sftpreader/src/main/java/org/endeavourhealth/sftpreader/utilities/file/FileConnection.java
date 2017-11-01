package org.endeavourhealth.sftpreader.utilities.file;

import com.jcraft.jsch.*;
import com.sun.javaws.exceptions.InvalidArgumentException;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.utilities.Connection;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.endeavourhealth.sftpreader.utilities.ConnectionDetails;
import org.endeavourhealth.sftpreader.utilities.sftp.SftpConnectionException;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Vector;

public class FileConnection extends Connection {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(FileConnection.class);

    public FileConnection(ConnectionDetails connectionDetails) {
        super(connectionDetails);
    }

    public void open() throws Exception {
        // No concept of 'open connection'
    }

    public List<RemoteFile> getFileList(String remotePath) throws Exception {
        LOG.info("Retrieving files from: " + remotePath);

        List<RemoteFile> ret = new Vector<>();

        if (remotePath.endsWith("*")) {
            //if the path ends with a *, then we want to recurse into sub-directories
            File dir = new File(remotePath);
            LOG.info("" + dir.getPath());
            File dirParent = dir.getParentFile();
            String dirName = dir.getName();
            if (!dirName.equals("*")) {
                throw new Exception("Invalid remote path");
            }

            File[] children = dirParent.listFiles();
            for (File child: children) {
                if (child.isDirectory()) {
                    String subDirPath = child.getPath();
                    List<RemoteFile> subDirFiles = getFileList(subDirPath);
                    ret.addAll(subDirFiles);
                }
            }

        } else {
            Path dir = new File(remotePath).toPath();
            DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
            for (Path file: stream) {
                File tempFile = file.toFile();
                if (tempFile.isFile()) {
                    RemoteFile remoteFile = new RemoteFile(tempFile.getName(),
                            remotePath,
                            tempFile.length(),
                            LocalDateTime.ofInstant(new Date(tempFile.lastModified()).toInstant(), ZoneId.systemDefault()));
                    ret.add(remoteFile);
                }
            }
        }

        return ret;
    }

    /*public List<RemoteFile> getFileList(String remotePath) throws Exception {
        LOG.info("Retrieving files from: " + remotePath);

        List<RemoteFile> ret = new Vector<RemoteFile>();
        Path dir = new File(remotePath).toPath();
        DirectoryStream<Path> stream = Files.newDirectoryStream(dir);
        for (Path file: stream) {
            File tempFile = file.toFile();
            if (tempFile.isFile()) {
                RemoteFile remoteFile = new RemoteFile(tempFile.getName(),
                        remotePath,
                        tempFile.length(),
                        LocalDateTime.ofInstant(new Date(tempFile.lastModified()).toInstant(), ZoneId.systemDefault()));
                ret.add(remoteFile);
            }
        }
        return ret;
    }*/

    public InputStream getFile(String remotePath) throws Exception {
        LOG.info("Retrieving single file: " + remotePath);
        return new FileInputStream(new File(remotePath));
    }

    public void deleteFile(String remotePath) throws Exception {
        LOG.info("Delete single file: " + remotePath);
        Path p = new File(remotePath).toPath();
        Files.delete(p);
    }

    /*public void cd(String remotePath) throws SftpException {
        // no concept of 'current location'
    }*/

    public void put(String localPath, String destinationPath) throws Exception {
        LOG.info("Save file: " + localPath + "==>" + destinationPath);
        Path from = new File(localPath).toPath();
        Path to = new File(destinationPath).toPath();
        Files.copy(from, to);
    }

    public void mkDir(String path) throws Exception {
        Path p = new File(path).toPath();
        Files.createDirectory(p);
    }

    public void close() {
        // no concept of 'close connection'
    }
}
