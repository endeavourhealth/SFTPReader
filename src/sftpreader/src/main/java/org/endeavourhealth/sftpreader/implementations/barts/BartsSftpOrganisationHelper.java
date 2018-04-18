package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class BartsSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {
        if (odsCode.equalsIgnoreCase("R1H")) {
            return "Barts Hospital Trust";

        } else {
            return "<UNKNOWN ORGANISATION>";
        }
    }
}
