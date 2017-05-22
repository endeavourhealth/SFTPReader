package org.endeavourhealth.sftpreader.management;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.endeavourhealth.sftpreader.Configuration;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

class ManagementHandler extends AbstractHandler {

    private Configuration configuration;

    public ManagementHandler(Configuration configuration) {
        this.configuration = configuration;
    }

    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        PrintWriter out = response.getWriter();

        out.println("SFTP Reader Management Interface");
        out.println("Running on instance" + configuration.getInstanceName());

        baseRequest.setHandled(true);
    }
}