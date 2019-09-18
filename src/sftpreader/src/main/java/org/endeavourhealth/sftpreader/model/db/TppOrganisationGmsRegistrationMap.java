package org.endeavourhealth.sftpreader.model.db;

public class TppOrganisationGmsRegistrationMap {

    private String organisationId = null;
    private String GmsOrganisationId = null;
    private Integer patientId = null;


    public TppOrganisationGmsRegistrationMap() {

    }

    public String getOrganisationId() {
        return organisationId;
    }

    public void setOrganisationId(String organisationId) {
        this.organisationId = organisationId;
    }

    public Integer getPatientId() {
        return patientId;
    }

    public void setPatientId(Integer patientId) {
        this.patientId = patientId;
    }

    public String getGmsOrganisationId() {
        return GmsOrganisationId;
    }

    public void setGmsOrganisationId(String gmsOrganisationId) {
        this.GmsOrganisationId = gmsOrganisationId;
    }
}
