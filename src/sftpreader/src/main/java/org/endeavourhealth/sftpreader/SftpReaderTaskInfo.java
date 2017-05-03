package org.endeavourhealth.sftpreader;

import org.endeavourhealth.sftpreader.model.db.DbConfiguration;

import java.time.LocalDateTime;

public class SftpReaderTaskInfo {

    private SftpReaderTask sftpReaderTask;
    private DbConfiguration dbConfiguration;
    private LocalDateTime nextScheduledDate;
    private String taskName;

    public SftpReaderTaskInfo(SftpReaderTask sftpReaderTask, DbConfiguration dbConfiguration) {
        this.sftpReaderTask = sftpReaderTask;
        this.dbConfiguration = dbConfiguration;
        this.nextScheduledDate = LocalDateTime.now();
        this.taskName = dbConfiguration.getConfigurationId();
    }

    public void runTask() throws InterruptedException {
        try {
            Thread thread = new Thread(sftpReaderTask, getTaskName());
            thread.start();
            thread.join();
        } finally {
            this.incrementScheduledDate();
        }
    }

    public LocalDateTime getNextScheduledDate() {
        return this.nextScheduledDate;
    }

    public void incrementScheduledDate() {
        this.nextScheduledDate = LocalDateTime.now().plusSeconds(dbConfiguration.getPollFrequencySeconds());
    }

    public String getTaskName() {
        return this.taskName;
    }
}
