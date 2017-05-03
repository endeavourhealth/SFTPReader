package org.endeavourhealth.sftpreader.model.db;

import org.apache.commons.io.FilenameUtils;

import java.util.List;

public class DbConfiguration {
    private String instanceId;
    private String instanceFriendlyName;
    private String interfaceTypeName;
    private int pollFrequencySeconds;
    private String localInstancePathPrefix;
    private String localInstancePath;

    private DbConfigurationSftp sftpConfiguration;
    private DbConfigurationPgp pgpConfiguration;
    private List<DbConfigurationKvp> kvpConfiguration;
    private List<String> interfaceFileTypes;

    public String getInstanceId() {
        return instanceId;
    }

    public DbConfiguration setInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    public String getInstanceFriendlyName() {
        return instanceFriendlyName;
    }

    public DbConfiguration setInstanceFriendlyName(String instanceFriendlyName) {
        this.instanceFriendlyName = instanceFriendlyName;
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

    public String getLocalInstancePathPrefix() {
        return localInstancePathPrefix;
    }

    public DbConfiguration setLocalInstancePathPrefix(String localInstancePathPrefix) {
        this.localInstancePathPrefix = localInstancePathPrefix;
        return this;
    }

    public String getFullLocalInstancePath() {
        return FilenameUtils.concat(getLocalInstancePathPrefix(), getLocalInstancePath());
    }

    public String getLocalInstancePath() {
        return localInstancePath;
    }

    public DbConfiguration setLocalInstancePath(String localInstancePath) {
        this.localInstancePath = localInstancePath;
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
}
