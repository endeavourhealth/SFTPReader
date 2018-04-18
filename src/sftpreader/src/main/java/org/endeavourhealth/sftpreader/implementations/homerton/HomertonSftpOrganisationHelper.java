package org.endeavourhealth.sftpreader.implementations.homerton;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class HomertonSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {
        return "RQX";
    }
}
