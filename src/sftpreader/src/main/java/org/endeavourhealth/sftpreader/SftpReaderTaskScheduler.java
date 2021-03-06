package org.endeavourhealth.sftpreader;

import org.endeavourhealth.core.application.ApplicationHeartbeatCallbackI;
import org.endeavourhealth.core.database.dal.audit.models.ApplicationHeartbeat;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SftpReaderTaskScheduler implements ApplicationHeartbeatCallbackI {

    private static final Logger LOG = LoggerFactory.getLogger(SftpReaderTaskScheduler.class);
    private static final DateTimeFormatter DATE_DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MMM-dd HH:mm:ss");
    private static final int THREAD_SLEEP_SECONDS = 5000;

    private Configuration configuration;
    private List<SftpReaderTaskInfo> tasks;
    private SftpReaderTaskInfo currentRunningTask = null;
    private Date currentTaskStarted = null;

    public SftpReaderTaskScheduler(Configuration configuration) {
        this.configuration = configuration;
    }

    public void start() throws InterruptedException {

        LOG.info("Starting SftpReaderTaskScheduler");

        this.tasks = createTasks(configuration);

        while (true) {

            for (SftpReaderTaskInfo task : tasks) {

                if (task.getNextScheduledDate().isBefore(LocalDateTime.now())) {

                    LOG.info("Starting SftpReaderTask " + task.getTaskName());
                    this.currentRunningTask = task;
                    this.currentTaskStarted = new Date();

                    LOG.trace("--------------------------------------------------");
                    task.runTask();
                    LOG.trace("--------------------------------------------------");

                    LOG.info("Completed SftpReaderTask " + task.getTaskName());
                    this.currentRunningTask = null;
                    this.currentTaskStarted = null;

                    LOG.trace("SftpReaderTask " + task.getTaskName() + " next scheduled for " + task.getNextScheduledDate().format(DATE_DISPLAY_FORMAT));
                }
            }

            Thread.sleep(THREAD_SLEEP_SECONDS);
        }
    }

    public void stop() {
    }

    private static List<SftpReaderTaskInfo> createTasks(Configuration configuration) {

        List<SftpReaderTaskInfo> tasks = new ArrayList<>();

        for (DbConfiguration dbConfiguration : configuration.getConfigurations()) {

            LOG.info("Creating SftpReaderTask for configuration " + dbConfiguration.getConfigurationId());

            SftpReaderTask sftpReaderTask = new SftpReaderTask(configuration, dbConfiguration.getConfigurationId());

            tasks.add(new SftpReaderTaskInfo(sftpReaderTask, dbConfiguration));
        }

        return tasks;
    }

    @Override
    public void populateIsBusy(ApplicationHeartbeat applicationHeartbeat) {
        if (this.currentRunningTask == null) {
            applicationHeartbeat.setBusy(Boolean.FALSE);
            applicationHeartbeat.setIsBusyDetail(null);

        } else {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
            String detailDesc = "Running " + currentRunningTask.getTaskName() + " since " + dateFormat.format(currentTaskStarted);

            applicationHeartbeat.setBusy(Boolean.TRUE);
            applicationHeartbeat.setIsBusyDetail(detailDesc);
        }
    }

    @Override
    public void populateInstanceNumber(ApplicationHeartbeat applicationHeartbeat) {
        //no instance number to set
    }
}
