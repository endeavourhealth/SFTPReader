package org.endeavourhealth.sftpreader;

import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class SftpReaderTaskScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(SftpReaderTaskScheduler.class);
    private static final DateTimeFormatter DATE_DISPLAY_FORMAT = DateTimeFormatter.ofPattern("yyyy-MMM-dd HH:mm:ss");
    private static final int THREAD_SLEEP_SECONDS = 5000;

    private Configuration configuration;
    private List<SftpReaderTaskInfo> tasks;

    public SftpReaderTaskScheduler(Configuration configuration) {
        this.configuration = configuration;
    }

    public void run() throws InterruptedException {

        LOG.info("Starting SftpReaderTaskScheduler");

        this.tasks = createTasks(configuration);

        while (true) {

            for (SftpReaderTaskInfo task : tasks) {

                if (task.getNextScheduledDate().isBefore(LocalDateTime.now())) {

                    LOG.info("Starting SftpReaderTask " + task.getTaskName());

                    LOG.trace("--------------------------------------------------");
                    task.runTask();
                    LOG.trace("--------------------------------------------------");

                    LOG.info("Completed SftpReaderTask " + task.getTaskName());

                    LOG.trace("SftpReaderTask " + task.getTaskName() + " next scheduled for " + task.getNextScheduledDate().format(DATE_DISPLAY_FORMAT));
                }
            }

            Thread.sleep(THREAD_SLEEP_SECONDS);
        }
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
}
