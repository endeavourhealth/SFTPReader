package org.endeavourhealth.sftpreader.model.db;

public class AddFileResult {
    private boolean fileAlreadyDownloaded;
    private int batchFileId;

    public boolean isFileAlreadyDownloaded() {
        return fileAlreadyDownloaded;
    }

    public AddFileResult setFileAlreadyDownloaded(boolean fileAlreadyDownloaded) {
        this.fileAlreadyDownloaded = fileAlreadyDownloaded;
        return this;
    }

    public int getBatchFileId() {
        return batchFileId;
    }

    public AddFileResult setBatchFileId(int batchFileId) {
        this.batchFileId = batchFileId;
        return this;
    }
}
