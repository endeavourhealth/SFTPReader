package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class VisionSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {

        try {
            OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(odsCode);
            if (odsOrg != null) {
                return odsOrg.getOrganisationName();

            } else {
                return null;
            }

        } catch (Exception Ex) {
            return null;
        }
    }
}
