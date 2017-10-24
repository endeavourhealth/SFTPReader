package org.endeavourhealth.sftpreader.implementations.homerton;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class HomertonSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) {
        return "RQX";
    }
}
