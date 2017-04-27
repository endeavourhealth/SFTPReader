package org.endeavourhealth.sftpreader.model.db;

public class DbConfigurationEds {
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

    public DbConfigurationEds setEdsUrl(String edsUrl) {
        this.edsUrl = edsUrl;
        return this;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public DbConfigurationEds setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
        return this;
    }

    public String getSoftwareContentType() {
        return softwareContentType;
    }

    public DbConfigurationEds setSoftwareContentType(String softwareContentType) {
        this.softwareContentType = softwareContentType;
        return this;
    }

    public boolean isUseKeycloak() {
        return useKeycloak;
    }

    public DbConfigurationEds setUseKeycloak(boolean useKeycloak) {
        this.useKeycloak = useKeycloak;
        return this;
    }

    public String getKeycloakTokenUri() {
        return keycloakTokenUri;
    }

    public DbConfigurationEds setKeycloakTokenUri(String keycloakTokenUri) {
        this.keycloakTokenUri = keycloakTokenUri;
        return this;
    }

    public String getKeycloakRealm() {
        return keycloakRealm;
    }

    public DbConfigurationEds setKeycloakRealm(String keycloakRealm) {
        this.keycloakRealm = keycloakRealm;
        return this;
    }

    public String getKeycloakUsername() {
        return keycloakUsername;
    }

    public DbConfigurationEds setKeycloakUsername(String keycloakUsername) {
        this.keycloakUsername = keycloakUsername;
        return this;
    }

    public String getKeycloakPassword() {
        return keycloakPassword;
    }

    public DbConfigurationEds setKeycloakPassword(String keycloakPassword) {
        this.keycloakPassword = keycloakPassword;
        return this;
    }

    public String getKeycloakClientId() {
        return keycloakClientId;
    }

    public DbConfigurationEds setKeycloakClientId(String keycloakClientId) {
        this.keycloakClientId = keycloakClientId;
        return this;
    }
}
