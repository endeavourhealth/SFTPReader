package org.endeavourhealth.sftpreader.model.db;

public class DbConfigurationSlack {
    private boolean enabled;
    private String slackUrl;
    private String messageTemplate;

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

    public String getMessageTemplate() {
        return messageTemplate;
    }

    public DbConfigurationSlack setMessageTemplate(String messageTemplate) {
        this.messageTemplate = messageTemplate;
        return this;
    }
}
