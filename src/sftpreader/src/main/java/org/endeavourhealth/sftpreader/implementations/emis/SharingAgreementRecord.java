package org.endeavourhealth.sftpreader.implementations.emis;

public class SharingAgreementRecord {

    private String guid;
    private boolean activated;
    private boolean disabled;
    private boolean deleted;

    public SharingAgreementRecord(String guid, boolean activated, boolean disabled, boolean deleted) {
        this.guid = guid;
        this.activated = activated;
        this.disabled = disabled;
        this.deleted = deleted;
    }

    public String getGuid() {
        return guid;
    }

    public boolean isActivated() {
        return activated;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return "Activated = " + activated + " Disabled = " + disabled + " Deleted = " + deleted;
    }
}
