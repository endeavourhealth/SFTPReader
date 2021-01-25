package org.endeavourhealth.sftpreader.model.db;

import java.util.Date;

public class BatchSplit {

    private int batchSplitId;
    private int batchId;
    private String configurationId;
    private String localRelativePath;
    private String organisationId;
    private boolean isHaveNotified;
    private Date notificationDate;
    private boolean isBulk;
    private boolean hasPatientData;

    private Batch batch;

    public BatchSplit() {}

    public int getBatchSplitId() {
        return batchSplitId;
    }

    public BatchSplit setBatchSplitId(int batchSplitId) {
        this.batchSplitId = batchSplitId;
        return this;
    }

    public int getBatchId() {
        return batchId;
    }

    public BatchSplit setBatchId(int batchId) {
        this.batchId = batchId;
        return this;
    }

    public String getConfigurationId() {
        return configurationId;
    }

    public BatchSplit setConfigurationId(String configurationId) {
        this.configurationId = configurationId;
        return this;
    }

    public String getLocalRelativePath() {
        return localRelativePath;
    }

    public BatchSplit setLocalRelativePath(String localRelativePath) {
        this.localRelativePath = localRelativePath;
        return this;
    }

    public String getOrganisationId() {
        return organisationId;
    }

    public BatchSplit setOrganisationId(String organisationId) {
        this.organisationId = organisationId;
        return this;
    }

    public boolean isHaveNotified() {
        return isHaveNotified;
    }

    public void setHaveNotified(boolean haveNotified) {
        isHaveNotified = haveNotified;
    }

    public Date getNotificationDate() {
        return notificationDate;
    }

    public void setNotificationDate(Date notificationDate) {
        this.notificationDate = notificationDate;
    }

    public Batch getBatch() {
        return batch;
    }

    public BatchSplit setBatch(Batch batch) {
        this.batch = batch;
        return this;
    }

    public boolean isBulk() {
        return isBulk;
    }

    public BatchSplit setBulk(boolean bulk) {
        isBulk = bulk;
        return this;
    }

    public boolean isHasPatientData() {
        return hasPatientData;
    }

    public BatchSplit setHasPatientData(boolean hasPatientData) {
        this.hasPatientData = hasPatientData;
        return this;
    }
}
