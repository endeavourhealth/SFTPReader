package org.endeavourhealth.sftpreader;

import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceConfigurationSlack;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SlackNotifier {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SlackNotifier.class);

    private Configuration configuration;
    private DbInstanceConfigurationSlack slackConfiguration;

    public SlackNotifier(Configuration configuration) {
        Validate.notNull(configuration, "configuration");
        Validate.notNull(configuration.getInstanceConfiguration().getSlackConfiguration(), "configuration.getDbGlobalConfiguration().getSlackConfiguration()");

        this.configuration = configuration;
        this.slackConfiguration = configuration.getInstanceConfiguration().getSlackConfiguration();
    }

    public void notifyStartup() {
        String message = Main.PROGRAM_DISPLAY_NAME + " started "
                + "(" + this.configuration.getInstanceName() + " instance, reading extracts "
                + this.configuration.getConfigurationIdsForDisplay() + ")";

        postMessage(message);
    }

    public void notifyShutdown() {
        String message = Main.PROGRAM_DISPLAY_NAME + " stopped (" + this.configuration.getInstanceName() + " instance)";
        postMessage(message);
    }

    public void notifyCompleteBatches(DbConfiguration dbConfiguration, List<Batch> batches) {
        for (Batch batch : batches)
            notifyCompleteBatch(dbConfiguration, batch);
    }

    public void notifyCompleteBatch(DbConfiguration dbConfiguration, Batch batch) {

        String configurationId = dbConfiguration.getConfigurationId();
        String friendlyName = dbConfiguration.getConfigurationFriendlyName();

        String message = friendlyName + " extract (" + configurationId + ") received";

        SftpSlackNotifier slackNotifier = ImplementationActivator.createSftpSlackNotifier();
        message += slackNotifier.getCompleteBatchMessageSuffix(batch);

        postMessage(message);
    }

    private void postMessage(String slackMessage) {
        try {
            if (!slackConfiguration.isEnabled())
                return;

            LOG.info("Posting message to slack: '" + slackMessage + "'");

            SlackApi slackApi = new SlackApi(slackConfiguration.getSlackUrl());
            slackApi.call(new SlackMessage(slackMessage));

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }
}
