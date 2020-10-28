package org.endeavourhealth.sftpreader.implementations.homerton;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class HomertonOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {

        if (odsCode.equalsIgnoreCase("RQX")) {
            return "Homerton University Hospital NHS Foundation Trust";

        } else {
            return "Unknown Organisation OdsCode received: "+odsCode;
        }
    }
}
