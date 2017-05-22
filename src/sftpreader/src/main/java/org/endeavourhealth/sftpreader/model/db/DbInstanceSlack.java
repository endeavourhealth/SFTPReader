package org.endeavourhealth.sftpreader.model.db;

public class DbInstanceSlack {
    private boolean enabled;
    private String slackUrl;

    public boolean isEnabled() {
        return enabled;
    }

    public DbInstanceSlack setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    public String getSlackUrl() {
        return slackUrl;
    }

    public DbInstanceSlack setSlackUrl(String slackUrl) {
        this.slackUrl = slackUrl;
        return this;
    }
}
