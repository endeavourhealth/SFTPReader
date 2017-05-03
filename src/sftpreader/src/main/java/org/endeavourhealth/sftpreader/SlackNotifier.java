package org.endeavourhealth.sftpreader;

import net.gpedro.integrations.slack.SlackApi;
import net.gpedro.integrations.slack.SlackMessage;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.ImplementationActivator;
import org.endeavourhealth.sftpreader.implementations.SftpSlackNotifier;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbGlobalConfigurationSlack;
import org.slf4j.LoggerFactory;

import java.util.List;


public class SlackNotifier {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SlackNotifier.class);

    private Configuration configuration;
    private DbGlobalConfigurationSlack slackConfiguration;

    public SlackNotifier(Configuration configuration) {
        Validate.notNull(configuration, "configuration");
        Validate.notNull(configuration.getGlobalConfiguration().getSlackConfiguration(), "configuration.getDbGlobalConfiguration().getSlackConfiguration()");

        this.configuration = configuration;
        this.slackConfiguration = configuration.getGlobalConfiguration().getSlackConfiguration();
    }

    public void notifyStartup() {
        postMessage("Service started (" + getInstanceNames() + ")");
    }

    public void notifyShutdown() {
        postMessage("Service stopped (" + getInstanceNames() + ")");
    }

    private String getInstanceNames() {
        return StringUtils.join(configuration.getInstanceNames(), ", ");
    }

    public void notifyCompleteBatches(DbConfiguration dbConfiguration, List<Batch> batches) {
        for (Batch batch : batches)
            notifyCompleteBatch(dbConfiguration, batch);
    }

    public void notifyCompleteBatch(DbConfiguration dbConfiguration, Batch batch) {

        String instanceName = dbConfiguration.getInstanceId();
        String friendlyName = dbConfiguration.getInstanceFriendlyName();

        String message = friendlyName + " extract (" + instanceName + ") received";

        SftpSlackNotifier slackNotifier = ImplementationActivator.createSftpSlackNotifier();
        message += slackNotifier.getCompleteBatchMessageSuffix(batch);

        postMessage(message);
    }

    private void postMessage(String slackMessage) {
        try {
            if (!slackConfiguration.isEnabled())
                return;

            SlackApi slackApi = new SlackApi(slackConfiguration.getSlackUrl());
            slackApi.call(new SlackMessage(slackMessage));

        } catch (Exception e) {
            LOG.warn("Error posting message to slack", e);
        }
    }
}
