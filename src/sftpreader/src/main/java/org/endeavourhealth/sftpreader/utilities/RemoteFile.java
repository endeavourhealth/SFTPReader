package org.endeavourhealth.sftpreader.utilities;

import org.apache.commons.io.FilenameUtils;

import java.time.LocalDateTime;

public class RemoteFile {
    private String filePath;
    private long fileSizeBytes;
    private LocalDateTime lastModified;

    public RemoteFile(String filePath, long fileSizeBytes, LocalDateTime lastModified) {
        this.filePath = filePath;
        this.fileSizeBytes = fileSizeBytes;
        this.lastModified = lastModified;
    }

    public String getFullPath() {
        return filePath;
    }

    public String getFilename() {
        return FilenameUtils.getName(filePath);
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public LocalDateTime getLastModified() {
        return lastModified;
    }
}
