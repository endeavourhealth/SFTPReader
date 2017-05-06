package org.endeavourhealth.sftpreader.model.db;

import java.util.List;

public class DbInstanceConfiguration {
    private List<String> configurationIds;
    private DbInstanceConfigurationSlack slackConfiguration;
    private DbInstanceConfigurationEds edsConfiguration;

    public List<String> getConfigurationIds() {
        return configurationIds;
    }

    public DbInstanceConfiguration setConfigurationIds(List<String> configurationIds) {
        this.configurationIds = configurationIds;
        return this;
    }

    public DbInstanceConfigurationSlack getSlackConfiguration() {
        return slackConfiguration;
    }

    public DbInstanceConfiguration setSlackConfiguration(DbInstanceConfigurationSlack slackConfiguration) {
        this.slackConfiguration = slackConfiguration;
        return this;
    }

    public DbInstanceConfigurationEds getEdsConfiguration() {
        return this.edsConfiguration;
    }

    public DbInstanceConfiguration setEdsConfiguration(DbInstanceConfigurationEds edsConfiguration) {
        this.edsConfiguration = edsConfiguration;
        return this;
    }
}
