package org.endeavourhealth.sftpreader.sources;

import com.google.common.base.Strings;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.FileInfo;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Map;
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

        List<FileInfo> listing = FileHelper.listFilesInSharedStorageWithInfo(remotePath);
        for (FileInfo fileInfo: listing) {
            String path = fileInfo.getFilePath();
            Date lastModified = fileInfo.getLastModified();
            long size = fileInfo.getSize();
            //LOG.info("Found " + path);

            String fileName = FilenameUtils.getName(path);
            if (Strings.isNullOrEmpty(fileName)) {
                LOG.info("Ignoring " + path + " as it's not a file");
                continue;
            }

            LocalDateTime lastModifiedLocalDate = LocalDateTime.ofInstant(lastModified.toInstant(), ZoneId.systemDefault());

            RemoteFile remoteFile = new RemoteFile(path, size, lastModifiedLocalDate);
            ret.add(remoteFile);
        }

        return ret;
    }

    /*public List<RemoteFile> getFileList(String remotePath) throws Exception {
        LOG.info("Retrieving files from: " + remotePath);

        List<RemoteFile> ret = new Vector<>();

        if (remotePath.endsWith("*")) {
            //if the path ends with a *, then we want to recurse into sub-directories
            File dir = new File(remotePath);
            //LOG.info("" + dir.getPath());
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
                    String fileName = tempFile.getName();
                    String filePath = FilenameUtils.concat(remotePath, fileName);
                    long len = tempFile.length();
                    LocalDateTime modified = LocalDateTime.ofInstant(new Date(tempFile.lastModified()).toInstant(), ZoneId.systemDefault());

                    RemoteFile remoteFile = new RemoteFile(filePath, len, modified);
                    ret.add(remoteFile);
                    LOG.info("Found " + remoteFile.getFullPath());
                }
            }
        }

        return ret;
    }*/



    public InputStream getFile(String remotePath) throws Exception {
        LOG.info("Retrieving single file: " + remotePath);
        return FileHelper.readFileFromSharedStorage(remotePath);
        //return new FileInputStream(new File(remotePath));
    }

    /* deleteFile, cd, mkdir methods now re-commented out,
     * put method remains uncommented for use in DataGenerator
     */

    /* public void deleteFile(String remotePath) throws Exception {
        LOG.info("Delete single file: " + remotePath);
        Path p = new File(remotePath).toPath();
        Files.delete(p);
    } */

    /* public void cd(String remotePath) throws Exception {
        // no concept of 'current location'
    } */

    public void put(String localPath, String destinationPath) throws Exception {
        LOG.info("Save file: " + localPath + "==>" + destinationPath);
        // Code commented out below cannot be used with AWS
        // Path from = new File(localPath).toPath();
        // Path to = new File(destinationPath).toPath();
        // Files.copy(from, to);
        File toFile = new File(destinationPath);
        FileHelper.writeFileToSharedStorage(localPath, toFile);
    }

    /* public void mkDir(String path) throws Exception {
        Path p = new File(path).toPath();
        Files.createDirectory(p);
    } */

    public void close() {
        // no concept of 'close connection'
    }
}
