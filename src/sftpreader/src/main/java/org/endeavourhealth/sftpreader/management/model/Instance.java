package org.endeavourhealth.sftpreader.management.model;

import java.time.LocalDateTime;

public class Instance {
    private String instanceName;
    private String hostname;
    private Integer httpManagementPort;
    private LocalDateTime lastConfigGetDate;

    public String getInstanceName() {
        return instanceName;
    }

    public Instance setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public String getHostname() {
        return hostname;
    }

    public Instance setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public Integer getHttpManagementPort() {
        return httpManagementPort;
    }

    public Instance setHttpManagementPort(Integer httpManagementPort) {
        this.httpManagementPort = httpManagementPort;
        return this;
    }

    public LocalDateTime getLastConfigGetDate() {
        return lastConfigGetDate;
    }

    public Instance setLastConfigGetDate(LocalDateTime lastConfigGetDate) {
        this.lastConfigGetDate = lastConfigGetDate;
        return this;
    }
}
