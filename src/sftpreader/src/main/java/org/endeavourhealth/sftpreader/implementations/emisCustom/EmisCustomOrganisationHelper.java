package org.endeavourhealth.sftpreader.implementations.emisCustom;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.endeavourhealth.sftpreader.implementations.emis.EmisOrganisationHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.EmisOrganisationMap;

public class EmisCustomOrganisationHelper extends SftpOrganisationHelper {
    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) throws Exception {
        return EmisOrganisationHelper.getOrganisationNameFromOdsCode(db, odsCode);
    }
}
