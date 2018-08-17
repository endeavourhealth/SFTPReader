package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.EmisOrganisationMap;

public class EmisCustomOrganisationHelper extends SftpOrganisationHelper {
    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) throws Exception {
        EmisOrganisationMap mapping = db.getEmisOrganisationMapForOdsCode(odsCode);
        if (mapping != null) {
            return mapping.getName();
        } else {
            return null;
        }
    }
}
