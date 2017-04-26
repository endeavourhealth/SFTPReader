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

    private DbConfigurationSftp dbConfigurationSftp;
    private DbConfigurationPgp dbConfigurationPgp;
    private DbConfigurationEds dbConfigurationEds;
    private DbConfigurationSlack dbConfigurationSlack;
    private List<DbConfigurationKvp> dbConfigurationKvp;
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

    public DbConfigurationPgp getDbConfigurationPgp() {
        return this.dbConfigurationPgp;
    }

    public DbConfiguration setDbConfigurationPgp(DbConfigurationPgp dbConfigurationPgp) {
        this.dbConfigurationPgp = dbConfigurationPgp;
        return this;
    }

    public DbConfigurationSftp getDbConfigurationSftp() {
        return this.dbConfigurationSftp;
    }

    public DbConfiguration setDbConfigurationSftp(DbConfigurationSftp dbConfigurationSftp) {
        this.dbConfigurationSftp = dbConfigurationSftp;
        return this;
    }

    public String getPgpFileExtensionFilter() {
        if (this.getDbConfigurationPgp() == null)
            return null;

        return this.getDbConfigurationPgp().getPgpFileExtensionFilter();
    }

    public List<String> getInterfaceFileTypes() {
        return this.interfaceFileTypes;
    }

    public DbConfiguration setInterfaceFileTypes(List<String> interfaceFileTypes) {
        this.interfaceFileTypes = interfaceFileTypes;
        return this;
    }

    public DbConfigurationEds getDbConfigurationEds() {
        return this.dbConfigurationEds;
    }

    public DbConfiguration setDbConfigurationEds(DbConfigurationEds dbConfigurationEds) {
        this.dbConfigurationEds = dbConfigurationEds;
        return this;
    }

    public List<DbConfigurationKvp> getDbConfigurationKvp() {
        return this.dbConfigurationKvp;
    }

    public DbConfiguration setDbConfigurationKvp(List<DbConfigurationKvp> dbConfigurationKvp) {
        this.dbConfigurationKvp = dbConfigurationKvp;
        return this;
    }

    public DbConfigurationSlack getDbConfigurationSlack() {
        return dbConfigurationSlack;
    }

    public DbConfiguration setDbConfigurationSlack(DbConfigurationSlack dbConfigurationSlack) {
        this.dbConfigurationSlack = dbConfigurationSlack;
        return this;
    }
}
