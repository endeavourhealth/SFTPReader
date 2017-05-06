package org.endeavourhealth.sftpreader.model.db;

public class DbInstanceConfigurationSlack {
    private boolean enabled;
    private String slackUrl;

    public boolean isEnabled() {
        return enabled;
    }

    public DbInstanceConfigurationSlack setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getSlackUrl() {
        return slackUrl;
    }

    public DbInstanceConfigurationSlack setSlackUrl(String slackUrl) {
        this.slackUrl = slackUrl;
        return this;
    }
}
