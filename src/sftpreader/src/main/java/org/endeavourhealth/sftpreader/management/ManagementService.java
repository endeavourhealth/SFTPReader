package org.endeavourhealth.sftpreader.management;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.endeavourhealth.sftpreader.Configuration;
import org.endeavourhealth.sftpreader.management.rest.ManagementRestEndpoint;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
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

        WebAppContext webAppContext = createWebAppContext();
        ServletContextHandler restServletContext = createRestServletContext();

        this.server.setHandler(webAppContext);

        //this.server.setHandler(restServletContext);
        this.server.start();
    }

    private WebAppContext createWebAppContext() throws URISyntaxException {
        WebAppContext context = new WebAppContext();
        context.setContextPath("/");
        context.setDescriptor(WEBAPP_RESOURCES_LOCATION + "/WEB-INF/web.xml");

        URL webAppDir = Thread.currentThread().getContextClassLoader().getResource(WEBAPP_RESOURCES_LOCATION);

        if (webAppDir == null)
            throw new RuntimeException(String.format("No %s directory was found into the JAR file", WEBAPP_RESOURCES_LOCATION));

        context.setResourceBase(webAppDir.toURI().toString());
        context.setParentLoaderPriority(true);

        return context;
    }

    private ServletContextHandler createRestServletContext() {
        ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/api");

        ServletHolder jerseyServlet = context.addServlet(org.glassfish.jersey.servlet.ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(0);

        jerseyServlet.setInitParameter(
                "jersey.config.server.provider.classnames",
                ManagementRestEndpoint.class.getCanonicalName());

        return context;
    }

    public void stop() throws Exception {

        if (server != null)
            this.server.stop();
    }
}
