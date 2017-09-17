package org.endeavourhealth.sftpreader.utilities;

import java.io.IOException;

public class ConnectionDetails {
    private String hostname;
    private int port;
    private String username;
    private String clientPrivateKey;
    private String clientPrivateKeyPassword;
    private String hostPublicKey;

    public String getHostname() {
        return hostname;
    }

    public ConnectionDetails setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ConnectionDetails setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ConnectionDetails setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getClientPrivateKey() {
        return clientPrivateKey;
    }

    public ConnectionDetails setClientPrivateKey(String clientPrivateKey) {
        this.clientPrivateKey = clientPrivateKey;
        return this;
    }

    public String getClientPrivateKeyPassword() {
        return clientPrivateKeyPassword;
    }

    public ConnectionDetails setClientPrivateKeyPassword(String clientPrivateKeyPassword) {
        this.clientPrivateKeyPassword = clientPrivateKeyPassword;
        return this;
    }

    public String getHostPublicKey() {
        return hostPublicKey;
    }

    public ConnectionDetails setHostPublicKey(String hostPublicKey) {
        this.hostPublicKey = hostPublicKey;
        return this;
    }

    public String getKnownHostsString() throws IOException {
        return this.getHostname() + " " + hostPublicKey + "\n";
    }
}
