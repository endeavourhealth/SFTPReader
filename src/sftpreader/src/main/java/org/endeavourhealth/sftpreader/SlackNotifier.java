package org.endeavourhealth.sftpreader;

import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackMessage;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfigurationSlack;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SlackNotifier {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SlackNotifier.class);

    private Configuration configuration;
    private DbConfigurationSlack dbConfigurationSlack;

    public SlackNotifier(Configuration configuration) {
        Validate.notNull(configuration, "configuration");
        Validate.notNull(configuration.getDbConfiguration().getDbConfigurationSlack(), "configuration.getDbConfiguration().getDbConfigurationSlack()");

        this.configuration = configuration;
        this.dbConfigurationSlack = configuration.getDbConfiguration().getDbConfigurationSlack();
    }

    public void notifyStartup() {
        postMessage("Service started (" + configuration.getInstanceName() + ")");
    }

    public void notifyShutdown() {
        postMessage("Service stopped (" + configuration.getInstanceName() + ")");
    }

    public void notifyCompleteBatches(List<Batch> batches) {
        for (Batch batch : batches)
            notifyCompleteBatch(batch);
    }

    public void notifyCompleteBatch(Batch batch) {

        String instanceName = configuration.getInstanceName();
        String friendlyName = configuration.getDbConfiguration().getInstanceFriendlyName();

        String message = friendlyName + " extract (" + instanceName + ") received";

        SftpSlackNotifier slackNotifier = ImplementationActivator.createSftpSlackNotifier();
        message += slackNotifier.getCompleteBatchMessageSuffix(batch);

        postMessage(message);
    }

    private void postMessage(String slackMessage) {
        try {
            if (!dbConfigurationSlack.isEnabled())
                return;

            SlackApi slackApi = new SlackApi(dbConfigurationSlack.getSlackUrl());
            slackApi.call(new SlackMessage(slackMessage));

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }
}
