package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class EmisSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) {
        return db.findEmisOrgNameFromOdsCode(odsCode);
    }
}
