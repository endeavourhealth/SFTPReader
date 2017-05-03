package org.endeavourhealth.sftpreader.model.db;

public class DbGlobalConfigurationEds {
    private String edsUrl;
    private String softwareContentType;
    private String softwareVersion;
    private boolean useKeycloak;
    private String keycloakTokenUri;
    private String keycloakRealm;
    private String keycloakUsername;
    private String keycloakPassword;
    private String keycloakClientId;

    public String getEdsUrl() {
        return edsUrl;
    }

    public DbGlobalConfigurationEds setEdsUrl(String edsUrl) {
        this.edsUrl = edsUrl;
        return this;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public DbGlobalConfigurationEds setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
        return this;
    }

    public String getSoftwareContentType() {
        return softwareContentType;
    }

    public DbGlobalConfigurationEds setSoftwareContentType(String softwareContentType) {
        this.softwareContentType = softwareContentType;
        return this;
    }

    public boolean isUseKeycloak() {
        return useKeycloak;
    }

    public DbGlobalConfigurationEds setUseKeycloak(boolean useKeycloak) {
        this.useKeycloak = useKeycloak;
        return this;
    }

    public String getKeycloakTokenUri() {
        return keycloakTokenUri;
    }

    public DbGlobalConfigurationEds setKeycloakTokenUri(String keycloakTokenUri) {
        this.keycloakTokenUri = keycloakTokenUri;
        return this;
    }

    public String getKeycloakRealm() {
        return keycloakRealm;
    }

    public DbGlobalConfigurationEds setKeycloakRealm(String keycloakRealm) {
        this.keycloakRealm = keycloakRealm;
        return this;
    }

    public String getKeycloakUsername() {
        return keycloakUsername;
    }

    public DbGlobalConfigurationEds setKeycloakUsername(String keycloakUsername) {
        this.keycloakUsername = keycloakUsername;
        return this;
    }

    public String getKeycloakPassword() {
        return keycloakPassword;
    }

    public DbGlobalConfigurationEds setKeycloakPassword(String keycloakPassword) {
        this.keycloakPassword = keycloakPassword;
        return this;
    }

    public String getKeycloakClientId() {
        return keycloakClientId;
    }

    public DbGlobalConfigurationEds setKeycloakClientId(String keycloakClientId) {
        this.keycloakClientId = keycloakClientId;
        return this;
    }
}
