package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.endeavourhealth.sftpreader.model.db.TppOrganisationMap;

public class TppSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) throws Exception {
        TppOrganisationMap mapping = db.findTppOrgNameFromOdsCode(odsCode);
        if (mapping != null) {
            return mapping.getName();
        } else {
            return null;
        }
    }
}
