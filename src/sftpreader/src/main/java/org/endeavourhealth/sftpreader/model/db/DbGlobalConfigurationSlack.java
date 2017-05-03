package org.endeavourhealth.sftpreader.model.db;

public class DbGlobalConfigurationSlack {
    private boolean enabled;
    private String slackUrl;

    public boolean isEnabled() {
        return enabled;
    }

    public DbGlobalConfigurationSlack setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getSlackUrl() {
        return slackUrl;
    }

    public DbGlobalConfigurationSlack setSlackUrl(String slackUrl) {
        this.slackUrl = slackUrl;
        return this;
    }
}
