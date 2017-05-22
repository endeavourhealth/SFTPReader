package org.endeavourhealth.sftpreader.management;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.endeavourhealth.sftpreader.Configuration;
import org.slf4j.LoggerFactory;

import java.net.URL;

public class ManagementService {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(ManagementService.class);
    public static final String WEBAPP_RESOURCES_LOCATION = "webapp";

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
        WebAppContext root = new WebAppContext();
        root.setContextPath("/");
        root.setDescriptor(WEBAPP_RESOURCES_LOCATION + "/WEB-INF/web.xml");

        URL webAppDir = Thread.currentThread().getContextClassLoader().getResource(WEBAPP_RESOURCES_LOCATION);
        if (webAppDir == null)
            throw new RuntimeException(String.format("No %s directory was found into the JAR file", WEBAPP_RESOURCES_LOCATION));

        root.setResourceBase(webAppDir.toURI().toString());
        root.setParentLoaderPriority(true);

        this.server.setHandler(root);
        this.server.start();
    }

    public void stop() throws Exception {

        if (server != null)
            this.server.stop();
    }
}
