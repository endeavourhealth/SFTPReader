package org.endeavourhealth.sftpreader.model.db;

public class TppOrganisationGmsRegistrationMap {

    private String organisationId = null;
    private String GmsOrganisationId = null;


    public TppOrganisationGmsRegistrationMap() {

    }

    public String getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(String organisationId) {
        this.organisationId = organisationId;
    }

    public String getGmsOrganisationId() {
        return GmsOrganisationId;
    }

    public void setGmsOrganisationId(String gmsOrganisationId) {
        this.GmsOrganisationId = gmsOrganisationId;
    }
}
