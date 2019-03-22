package org.endeavourhealth.sftpreader.model.db;

public class BatchFile {
    private int batchId;
    private int batchFileId;
    private String fileTypeIdentifier;
    private String filename;
    private long remoteSizeBytes;
    private boolean isDownloaded;
    /*private long localSizeBytes;
    private boolean requiresDecryption;
    private boolean isDecrypted;
    private String decryptedFilename;
    private long decryptedSizeBytes;*/
    private boolean isDeleted;

    public int getBatchId() {
        return batchId;
    }

    public BatchFile setBatchId(int batchId) {
        this.batchId = batchId;
        return this;
    }

    public int getBatchFileId() {
        return batchFileId;
    }

    public BatchFile setBatchFileId(int batchFileId) {
        this.batchFileId = batchFileId;
        return this;
    }

    public String getFileTypeIdentifier() {
        return fileTypeIdentifier;
    }

    public BatchFile setFileTypeIdentifier(String fileTypeIdentifier) {
        this.fileTypeIdentifier = fileTypeIdentifier;
        return this;
    }

    public String getFilename() {
        return filename;
    }

    public BatchFile setFilename(String filename) {
        this.filename = filename;
        return this;
    }

    public long getRemoteSizeBytes() {
        return remoteSizeBytes;
    }

    public BatchFile setRemoteSizeBytes(long remoteSizeBytes) {
        this.remoteSizeBytes = remoteSizeBytes;
        return this;
    }

    public boolean isDownloaded() {
        return isDownloaded;
    }

    public BatchFile setDownloaded(boolean downloaded) {
        isDownloaded = downloaded;
        return this;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public BatchFile setDeleted(boolean deleted) {
        isDeleted = deleted;
        return this;
    }
}
