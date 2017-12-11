package org.endeavourhealth.sftpreader.model.db;

import org.apache.commons.io.FilenameUtils;

import java.util.List;

public class DbConfiguration {
    private String configurationId;
    private String configurationFriendlyName;
    private String interfaceTypeName;
    private int pollFrequencySeconds;
    //private String localRootPathPrefix;
    private String localRootPath;
    private String softwareContentType;
    private String softwareVersion;


    private DbConfigurationSftp sftpConfiguration;
    private DbConfigurationPgp pgpConfiguration;
    private List<DbConfigurationKvp> kvpConfiguration;
    private List<String> interfaceFileTypes;

    public String getConfigurationId() {
        return configurationId;
    }

    public DbConfiguration setConfigurationId(String configurationId) {
        this.configurationId = configurationId;
        return this;
    }

    public String getConfigurationFriendlyName() {
        return configurationFriendlyName;
    }

    public DbConfiguration setConfigurationFriendlyName(String configurationFriendlyName) {
        this.configurationFriendlyName = configurationFriendlyName;
        return this;
    }

    public String getInterfaceTypeName() {
        return interfaceTypeName;
    }

    public DbConfiguration setInterfaceTypeName(String interfaceTypeName) {
        this.interfaceTypeName = interfaceTypeName;
        return this;
    }

    public int getPollFrequencySeconds() {
        return pollFrequencySeconds;
    }

    public DbConfiguration setPollFrequencySeconds(int pollFrequencySeconds) {
        this.pollFrequencySeconds = pollFrequencySeconds;
        return this;
    }

    /*public String getLocalRootPathPrefix() {
        return localRootPathPrefix;
    }

    public DbConfiguration setLocalRootPathPrefix(String localRootPathPrefix) {
        this.localRootPathPrefix = localRootPathPrefix;
        return this;
    }*/

    /*public String getFullLocalRootPath() {
        return FilenameUtils.concat(getLocalRootPathPrefix(), getLocalRootPath());
    }*/

    public String getLocalRootPath() {
        return localRootPath;
    }

    public DbConfiguration setLocalRootPath(String localRootPath) {
        this.localRootPath = localRootPath;
        return this;
    }

    public DbConfigurationPgp getPgpConfiguration() {
        return this.pgpConfiguration;
    }

    public DbConfiguration setPgpConfiguration(DbConfigurationPgp pgpConfiguration) {
        this.pgpConfiguration = pgpConfiguration;
        return this;
    }

    public DbConfigurationSftp getSftpConfiguration() {
        return this.sftpConfiguration;
    }

    public DbConfiguration setSftpConfiguration(DbConfigurationSftp sftpConfiguration) {
        this.sftpConfiguration = sftpConfiguration;
        return this;
    }

    public String getPgpFileExtensionFilter() {
        if (this.getPgpConfiguration() == null)
            return null;

        return this.getPgpConfiguration().getPgpFileExtensionFilter();
    }

    public List<String> getInterfaceFileTypes() {
        return this.interfaceFileTypes;
    }

    public DbConfiguration setInterfaceFileTypes(List<String> interfaceFileTypes) {
        this.interfaceFileTypes = interfaceFileTypes;
        return this;
    }

    public List<DbConfigurationKvp> getDbConfigurationKvp() {
        return this.kvpConfiguration;
    }

    public DbConfiguration setDbConfigurationKvp(List<DbConfigurationKvp> kvpConfiguration) {
        this.kvpConfiguration = kvpConfiguration;
        return this;
    }

    public String getSoftwareVersion() {
        return softwareVersion;
    }

    public DbConfiguration setSoftwareVersion(String softwareVersion) {
        this.softwareVersion = softwareVersion;
        return this;
    }

    public String getSoftwareContentType() {
        return softwareContentType;
    }

    public DbConfiguration setSoftwareContentType(String softwareContentType) {
        this.softwareContentType = softwareContentType;
        return this;
    }


}
