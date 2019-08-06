package org.endeavourhealth.sftpreader.model.db;

import com.google.common.base.Strings;

import java.util.Date;

public class ConfigurationPollingAttempt {
    private String configurationId;
    private Date attemptStarted;
    private Date attemptFinished;
    private String errorText;
    private int filesDownloaded;
    private int batchesCompleted;
    private int batchSplitsNotifiedOk;
    private int batchSplitsNotifiedFailure;

    public ConfigurationPollingAttempt() {
    }

    public String getConfigurationId() {
        return configurationId;
    }

    public void setConfigurationId(String configurationId) {
        this.configurationId = configurationId;
    }

    public Date getAttemptStarted() {
        return attemptStarted;
    }

    public void setAttemptStarted(Date attemptStarted) {
        this.attemptStarted = attemptStarted;
    }

    public Date getAttemptFinished() {
        return attemptFinished;
    }

    public void setAttemptFinished(Date attemptFinished) {
        this.attemptFinished = attemptFinished;
    }

    public String getErrorText() {
        return errorText;
    }

    public void setErrorText(String errorText) {
        this.errorText = errorText;
    }

    public int getFilesDownloaded() {
        return filesDownloaded;
    }

    public void setFilesDownloaded(int filesDownloaded) {
        this.filesDownloaded = filesDownloaded;
    }

    public int getBatchesCompleted() {
        return batchesCompleted;
    }

    public void setBatchesCompleted(int batchesCompleted) {
        this.batchesCompleted = batchesCompleted;
    }

    public int getBatchSplitsNotifiedOk() {
        return batchSplitsNotifiedOk;
    }

    public void setBatchSplitsNotifiedOk(int batchSplitsNotifiedOk) {
        this.batchSplitsNotifiedOk = batchSplitsNotifiedOk;
    }

    public int getBatchSplitsNotifiedFailure() {
        return batchSplitsNotifiedFailure;
    }

    public void setBatchSplitsNotifiedFailure(int batchSplitsNotifiedFailure) {
        this.batchSplitsNotifiedFailure = batchSplitsNotifiedFailure;
    }

    public boolean hasError() {
        return !Strings.isNullOrEmpty(errorText);
    }
}
