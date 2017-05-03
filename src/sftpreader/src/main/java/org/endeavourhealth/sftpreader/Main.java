package org.endeavourhealth.sftpreader;

import org.endeavourhealth.common.config.ConfigManagerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

	private static final String PROGRAM_DISPLAY_NAME = "SFTP Reader";
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
    private static Configuration configuration;
    private static SlackNotifier slackNotifier;

	public static void main(String[] args) {
		try {
            configuration = Configuration.getInstance();

            LOG.info("--------------------------------------------------");
            LOG.info(PROGRAM_DISPLAY_NAME);
            LOG.info("--------------------------------------------------");

            getSlackNotifier().notifyStartup();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> shutdown()));

            SftpReaderTaskScheduler sftpReaderTaskScheduler = new SftpReaderTaskScheduler(configuration);
            sftpReaderTaskScheduler.run();

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

