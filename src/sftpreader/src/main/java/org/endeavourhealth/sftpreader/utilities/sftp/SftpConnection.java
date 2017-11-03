package org.endeavourhealth.sftpreader.utilities.sftp;

import com.jcraft.jsch.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.utilities.Connection;
import org.endeavourhealth.sftpreader.utilities.ConnectionDetails;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;

public class SftpConnection extends Connection {
    //private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SftpConnection.class);

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
        this.jSch = new JSch();

        jSch.addIdentity("client-private-key", getConnectionDetails().getClientPrivateKey().getBytes(), null, getConnectionDetails().getClientPrivateKeyPassword().getBytes());

        String hostPublicKey = getConnectionDetails().getHostPublicKey();
        if (StringUtils.isNotBlank(hostPublicKey)) {
            String knownHosts = getConnectionDetails().getKnownHostsString();
            jSch.setKnownHosts(new ByteArrayInputStream(knownHosts.getBytes()));
            this.session = jSch.getSession(getConnectionDetails().getUsername(), getConnectionDetails().getHostname(), getConnectionDetails().getPort());
        } else {
            this.session = jSch.getSession(getConnectionDetails().getUsername(), getConnectionDetails().getHostname(), getConnectionDetails().getPort());
            this.session.setConfig("StrictHostKeyChecking", "no");
        }
        this.session.connect();

        this.channel = (ChannelSftp)session.openChannel("sftp");
        this.channel.connect();

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
                                "", //"\\", //trying alternatives that work on all known SFTP servers
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
        File f = new File(remotePath);
        String name = f.getName();

        return channel.get(name);
    }

    public void deleteFile(String remotePath) throws SftpException {
        channel.rm(remotePath);
    }

    /*public void cd(String remotePath) throws SftpException {
        channel.cd(remotePath);
    }*/

    public void put(String localPath, String destinationPath) throws SftpException {
        channel.put(localPath, destinationPath);
    }

    public void mkDir(String path) throws SftpException {
        channel.mkdir(path);
    }

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
