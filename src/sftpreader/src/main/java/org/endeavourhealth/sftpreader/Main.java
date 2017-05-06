package org.endeavourhealth.sftpreader;

import org.endeavourhealth.common.config.ConfigManagerException;
import org.endeavourhealth.common.postgres.PgAppLock.PgAppLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

	public static final String PROGRAM_DISPLAY_NAME = "SFTP Reader";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static SlackNotifier slackNotifier;

	public static void main(String[] args) {
		try {
            configuration = Configuration.getInstance();

            PgAppLock pgAppLock = new PgAppLock(configuration.getInstanceName(), configuration.getNonPooledDatabaseConnection());
            try {

                LOG.info("--------------------------------------------------");
                LOG.info(PROGRAM_DISPLAY_NAME);
                LOG.info("--------------------------------------------------");

                LOG.info("Instance " + configuration.getInstanceName() + " on host " + configuration.getMachineName());
                LOG.info("Processing configuration(s): " + configuration.getConfigurationIdsForDisplay());

                getSlackNotifier().notifyStartup();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

                SftpReaderTaskScheduler sftpReaderTaskScheduler = new SftpReaderTaskScheduler(configuration);
                sftpReaderTaskScheduler.run();

            } finally {
                pgAppLock.releaseLock();
            }
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

            getSlackNotifier().notifyShutdown();

        } catch (Exception e) {
            printToErrorConsole("Exception occurred during shutdown", e);
            LOG.error("Exception occurred during shutdown", e);
        }
    }

    private static void printToErrorConsole(String message, Exception e) {
        System.err.println(message + " [" + e.getClass().getName() + "] " + e.getMessage());
    }

    private static SlackNotifier getSlackNotifier() {
	    if (slackNotifier == null)
	        slackNotifier = new SlackNotifier(configuration);

	    return slackNotifier;
    }
}

