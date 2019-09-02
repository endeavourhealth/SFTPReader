package org.endeavourhealth.sftpreader.sources;

import com.google.common.base.Strings;
import com.jcraft.jsch.*;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class SftpConnection extends Connection {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpConnection.class);

    //private ConnectionDetails connectionDetails;
    private JSch jSch;
    private Session session;
    private ChannelSftp channel;

    public SftpConnection(ConnectionDetails connectionDetails) {
        super(connectionDetails);
        Validate.notEmpty(connectionDetails.getHostname(), "hostname is empty");
        Validate.notEmpty(connectionDetails.getUsername(), "username is empty");
        Validate.isTrue(connectionDetails.getPort() > 0, "port must be positive");
    }

    public void open() throws JSchException, IOException, SftpConnectionException {

        //JSch.setLogger(new Logger());   //uncomment to enable JSch verbose SSH logging

        this.jSch = new JSch();

        String prvKey = getConnectionDetails().getClientPrivateKey().trim();
        String pw = getConnectionDetails().getClientPrivateKeyPassword().trim();
        if (!Strings.isNullOrEmpty(prvKey)) {
            jSch.addIdentity("client-private-key", prvKey.getBytes(), null, pw.getBytes());
        }

        //NOTE: To find the public host key, use SSH sftp to connect to the server and then copy the
        //record from the ~/.ssh/known_hosts file. It's easier to work out the correct record if the known_hosts
        //is first backed up, then emptied, then you know exactly which record is for the new server
        String hostPublicKey = getConnectionDetails().getHostPublicKey();
        if (StringUtils.isNotBlank(hostPublicKey)) {
            String knownHosts = getConnectionDetails().getKnownHostsString();
            jSch.setKnownHosts(new ByteArrayInputStream(knownHosts.getBytes()));
            this.session = jSch.getSession(getConnectionDetails().getUsername(), getConnectionDetails().getHostname(), getConnectionDetails().getPort());
        } else {
            this.session = jSch.getSession(getConnectionDetails().getUsername(), getConnectionDetails().getHostname(), getConnectionDetails().getPort());
            this.session.setConfig("StrictHostKeyChecking", "no");
        }

        // no private key supplied and using standard password authentication
        if (Strings.isNullOrEmpty(prvKey) && !Strings.isNullOrEmpty(pw)) {
            session.setPassword(pw);
        }

        LOG.trace("Session set up");

        this.session.connect();
        LOG.trace("Session connected");

        this.channel = (ChannelSftp)session.openChannel("sftp");
        this.channel.connect();
        LOG.trace("Channel connected");

        //adding this to try to get past an error with new Emis server
        /*KnownHosts knownHosts = (KnownHosts)jSch.getHostKeyRepository();
        for (HostKey key: knownHosts.getHostKey()) {
            LOG.info("Public key: " + key.getKey());
            LOG.info("Public fingerprint: " + key.getFingerPrint(jSch));
        }

        this.session.setUserInfo(new TestUserInfo());

        try {
            this.session.connect();

            this.channel = (ChannelSftp)session.openChannel("sftp");
            this.channel.connect();
        } catch (Exception ex) {

            HostKey serverHostKey = session.getHostKey();
            LOG.info("Server Public key: " + serverHostKey.getKey());
            LOG.info("Server Public fingerprint: " + serverHostKey.getFingerPrint(jSch));

            throw ex;
        }*/
    }

    public static class Logger implements com.jcraft.jsch.Logger {
        static java.util.Hashtable name=new java.util.Hashtable();
        static{
            name.put(new Integer(DEBUG), "DEBUG: ");
            name.put(new Integer(INFO), "INFO: ");
            name.put(new Integer(WARN), "WARN: ");
            name.put(new Integer(ERROR), "ERROR: ");
            name.put(new Integer(FATAL), "FATAL: ");
        }
        public boolean isEnabled(int level){
            return true;
        }
        public void log(int level, String message){
            LOG.info(name.get(new Integer(level)).toString());
            LOG.info(message);
        }
    }

    @SuppressWarnings("unchecked")
    public List<RemoteFile> getFileList(String remotePath) throws SftpException {

        //an error is raised if we try to list the files without first changing into the directory
        channel.cd(remotePath);

        //trying alternatives that work on all known SFTP servers
        Vector<ChannelSftp.LsEntry> fileList = channel.ls(".");
        //Vector<ChannelSftp.LsEntry> fileList = channel.ls("\\");

        return fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .map(t ->
                        new RemoteFile(t.getFilename(),
                                t.getAttrs().getSize(),
                                LocalDateTime.ofInstant(new Date((long)t.getAttrs().getMTime() * 1000L).toInstant(), ZoneId.systemDefault())
                        )
                )
                .collect(Collectors.toList());
    }

    /*@SuppressWarnings("unchecked")
    public List<RemoteFile> getFileList(String remotePath) throws SftpException {

        Vector<ChannelSftp.LsEntry> fileList = channel.ls(remotePath);

        return fileList
                .stream()
                .filter(t -> !t.getAttrs().isDir())
                .map(t ->
                        new RemoteFile(t.getFilename(),
                                remotePath,
                                t.getAttrs().getSize(),
                                LocalDateTime.ofInstant(new Date((long)t.getAttrs().getMTime() * 1000L).toInstant(), ZoneId.systemDefault())
                        )
                )
                .collect(Collectors.toList());
    }*/

    public InputStream getFile(String remotePath) throws SftpException {

        //when listing the files we change into the directory, so want to remove the path and just download by filename
        /*File f = new File(remotePath);
        String name = f.getName();*/
        String name = FilenameUtils.getName(remotePath);

        return channel.get(name);
    }

    /* deleteFile, cd, mkdir methods now re-commented out,
     * put method remains uncommented for use in DataGenerator
     */

    /* public void deleteFile(String remotePath) throws SftpException {
        channel.rm(remotePath);
    } */

    /* public void cd(String remotePath) throws SftpException {
        channel.cd(remotePath);
    } */

    public void put(String localPath, String destinationPath) throws SftpException {
        channel.put(localPath, destinationPath);
    }

    /* public void mkDir(String path) throws SftpException {
        channel.mkdir(path);
    } */

    public void close() {
        if (channel != null && channel.isConnected())
            channel.disconnect();

        if (session != null && session.isConnected())
            session.disconnect();
    }

    /*class TestUserInfo implements UserInfo {

        @Override
        public String getPassphrase() {
            LOG.info("UserInfo getPassphrase");
            return null;
        }

        @Override
        public String getPassword() {
            LOG.info("UserInfo getPassword");
            return "";
        }

        @Override
        public boolean promptPassword(String message) {
            LOG.info("UserInfo promptPassword: " + message);
            return true;
        }

        @Override
        public boolean promptPassphrase(String message) {
            LOG.info("UserInfo promptPassphrase: " + message);
            return false;
        }

        @Override
        public boolean promptYesNo(String message) {
            LOG.info("UserInfo promptYesNo: " + message);
            return true;
        }

        @Override
        public void showMessage(String message) {
            LOG.info("UserInfo showMessage: " + message);
        }
    }*/
}
