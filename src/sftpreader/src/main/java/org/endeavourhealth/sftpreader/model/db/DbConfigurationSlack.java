package org.endeavourhealth.sftpreader.model.db;

public class DbConfigurationSlack {
    private boolean enabled;
    private String slackUrl;

    public boolean isEnabled() {
        return enabled;
    }

    public DbConfigurationSlack setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getSlackUrl() {
        return slackUrl;
    }

    public DbConfigurationSlack setSlackUrl(String slackUrl) {
        this.slackUrl = slackUrl;
        return this;
    }
}
