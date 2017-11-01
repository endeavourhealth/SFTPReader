package org.endeavourhealth.sftpreader.model.db;

import java.util.Date;

public class EmisOrganisationMap {
    private String guid = null;
    private String name = null;
    private String odsCode = null;
    private Date startDate = null;


    public EmisOrganisationMap() {
    }

    public String getGuid() {
        return guid;
    }

    public EmisOrganisationMap setGuid(String guid) {
        this.guid = guid;
        return this;
    }

    public String getName() {
        return name;
    }

    public EmisOrganisationMap setName(String name) {
        this.name = name;
        return this;
    }

    public String getOdsCode() {
        return odsCode;
    }

    public EmisOrganisationMap setOdsCode(String odsCode) {
        this.odsCode = odsCode;
        return this;
    }

    public Date getStartDate() {
        return startDate;
    }

    public EmisOrganisationMap setStartDate(Date startDate) {
        this.startDate = startDate;
        return this;
    }

}
