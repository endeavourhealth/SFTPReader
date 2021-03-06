package org.endeavourhealth.sftpreader.model.db;

import java.util.Date;

public class EmisOrganisationMap {
    private String guid = null;
    private String name = null;
    private String odsCode = null;

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

    @Override
    public String toString() {
        return "Guid = " + guid + " name = " + name + " odsCode = " + odsCode;
    }
}
