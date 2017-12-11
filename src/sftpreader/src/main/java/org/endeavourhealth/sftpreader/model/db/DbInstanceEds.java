package org.endeavourhealth.sftpreader.model.db;

public class DbInstanceEds {
    private String edsUrl;
    /*private String softwareContentType;
    private String softwareVersion;*/
    private boolean useKeycloak;
    private String keycloakTokenUri;
    private String keycloakRealm;
    private String keycloakUsername;
    private String keycloakPassword;
    private String keycloakClientId;
    private String tempDirectory;
    private String sharedStoragePath;

    public String getEdsUrl() {
        return edsUrl;
    }

    public DbInstanceEds setEdsUrl(String edsUrl) {
        this.edsUrl = edsUrl;
        return this;
    }

    /*public String getSoftwareVersion() {
        return softwareVersion;
    }

    public DbInstanceEds setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
        return this;
    }

    public String getSoftwareContentType() {
        return softwareContentType;
    }

    public DbInstanceEds setSoftwareContentType(String softwareContentType) {
        this.softwareContentType = softwareContentType;
        return this;
    }*/

    public boolean isUseKeycloak() {
        return useKeycloak;
    }

    public DbInstanceEds setUseKeycloak(boolean useKeycloak) {
        this.useKeycloak = useKeycloak;
        return this;
    }

    public String getKeycloakTokenUri() {
        return keycloakTokenUri;
    }

    public DbInstanceEds setKeycloakTokenUri(String keycloakTokenUri) {
        this.keycloakTokenUri = keycloakTokenUri;
        return this;
    }

    public String getKeycloakRealm() {
        return keycloakRealm;
    }

    public DbInstanceEds setKeycloakRealm(String keycloakRealm) {
        this.keycloakRealm = keycloakRealm;
        return this;
    }

    public String getKeycloakUsername() {
        return keycloakUsername;
    }

    public DbInstanceEds setKeycloakUsername(String keycloakUsername) {
        this.keycloakUsername = keycloakUsername;
        return this;
    }

    public String getKeycloakPassword() {
        return keycloakPassword;
    }

    public DbInstanceEds setKeycloakPassword(String keycloakPassword) {
        this.keycloakPassword = keycloakPassword;
        return this;
    }

    public String getKeycloakClientId() {
        return keycloakClientId;
    }

    public DbInstanceEds setKeycloakClientId(String keycloakClientId) {
        this.keycloakClientId = keycloakClientId;
        return this;
    }

    public String getTempDirectory() {
        return tempDirectory;
    }

    public DbInstanceEds setTempDirectory(String tempDirectory) {
        this.tempDirectory = tempDirectory;
        return this;
    }

    public String getSharedStoragePath() {
        return sharedStoragePath;
    }

    public DbInstanceEds setSharedStoragePath(String sharedStoragePath) {
        this.sharedStoragePath = sharedStoragePath;
        return this;
    }
}
