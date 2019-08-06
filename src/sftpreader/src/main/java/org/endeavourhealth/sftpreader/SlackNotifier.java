package org.endeavourhealth.sftpreader;

import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackMessage;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SlackNotifier {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SlackNotifier.class);

    public static void notifyCompleteBatch(DbConfiguration dbConfiguration, Batch batch) throws Exception {

        String configurationId = dbConfiguration.getConfigurationId();
        String friendlyName = dbConfiguration.getConfigurationFriendlyName();

        String message = friendlyName + " extract (" + configurationId + ") received";

        SftpSlackNotifier slackNotifier = ImplementationActivator.createSftpSlackNotifier(dbConfiguration);
        message += slackNotifier.getCompleteBatchMessageSuffix(batch);

        postMessage(message);
    }

    public static void postMessage(String slackMessage) {
        try {
            LOG.info("Posting message to slack: '" + slackMessage + "'");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, slackMessage);

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }

    public static void postMessage(String slackMessage, String attachmentStr) {
        try {
            LOG.info("Posting message to slack: '" + slackMessage + "'");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, slackMessage, attachmentStr);

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }

    public static void postMessage(String slackMessage, Throwable t) {

        //need an Exception for SlackHelper, so wrap in a runtime exception if necessary
        Exception ex;
        if (t instanceof Exception) {
            ex = (Exception)t;
        } else {
            ex = new RuntimeException(t);
        }

        try {
            LOG.info("Posting message to slack: '" + slackMessage + "'");
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, slackMessage, ex);

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }
}
