package org.endeavourhealth.sftpreader.model.db;

public class DbGlobalConfiguration {
    private DbGlobalConfigurationSlack slackConfiguration;
    private DbGlobalConfigurationEds edsConfiguration;

    public DbGlobalConfigurationSlack getSlackConfiguration() {
        return slackConfiguration;
    }

    public DbGlobalConfiguration setSlackConfiguration(DbGlobalConfigurationSlack slackConfiguration) {
        this.slackConfiguration = slackConfiguration;
        return this;
    }

    public DbGlobalConfigurationEds getEdsConfiguration() {
        return this.edsConfiguration;
    }

    public DbGlobalConfiguration setEdsConfiguration(DbGlobalConfigurationEds edsConfiguration) {
        this.edsConfiguration = edsConfiguration;
        return this;
    }
}
