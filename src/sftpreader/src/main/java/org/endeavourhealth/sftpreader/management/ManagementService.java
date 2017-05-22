package org.endeavourhealth.sftpreader.management;

import org.eclipse.jetty.server.Server;
import org.endeavourhealth.sftpreader.Configuration;
import org.slf4j.LoggerFactory;

public class ManagementService {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ManagementService.class);

    private Configuration configuration;
    private Server server;
    private Integer httpPort;

    public ManagementService(Configuration configuration) {
        this.configuration = configuration;
    }

    public void start() throws Exception {

        this.httpPort = this.configuration.getInstanceConfiguration().getHttpManagementPort();

        if (httpPort == null) {
            LOG.info("Management interface NOT starting as http management port not set");
            return;
        }

        LOG.info("Starting http management interface on port " + httpPort.toString());

        this.server = new Server(httpPort.intValue());
        this.server.setHandler(new ManagementHandler(configuration));
        this.server.start();
    }

    public void stop() throws Exception {

        if (server != null)
            this.server.stop();
    }
}
