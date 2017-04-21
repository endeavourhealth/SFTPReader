package org.endeavourhealth.sftpreader;

import org.endeavourhealth.common.config.ConfigManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;

public class Main {

	private static final String PROGRAM_DISPLAY_NAME = "SFTP Reader";
    private static final String TIMER_THREAD_NAME = "SftpTask";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;

	public static void main(String[] args) {
		try {
            configuration = Configuration.getInstance();

            LOG.info("--------------------------------------------------");
            LOG.info(PROGRAM_DISPLAY_NAME);
            LOG.info("--------------------------------------------------");

            SftpTask sftpTask = new SftpTask(configuration);

            Timer timer = new Timer(TIMER_THREAD_NAME);
            timer.scheduleAtFixedRate(sftpTask, 0, configuration.getDbConfiguration().getPollFrequencySeconds() * 1000);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

        } catch (ConfigManagerException cme) {
            printToErrorConsole("Fatal exception occurred initializing ConfigManager", cme);
            LOG.error("Fatal exception occurred initializing ConfigManager", cme);
            System.exit(-2);
        }
        catch (Exception e) {
            LOG.error("Fatal exception occurred", e);
            System.exit(-1);
        }
	}

    private static void shutdown() {
        try {
            LOG.info("Shutting down...");

        } catch (Exception e) {
            printToErrorConsole("Exception occurred during shutdown", e);
            LOG.error("Exception occurred during shutdown", e);
        }
    }

    private static void printToErrorConsole(String message, Exception e) {
        System.err.println(message + " [" + e.getClass().getName() + "] " + e.getMessage());
    }
}

