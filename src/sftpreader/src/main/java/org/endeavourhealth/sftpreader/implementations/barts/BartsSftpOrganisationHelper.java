package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class BartsSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) {
        if (odsCode.equalsIgnoreCase("R1H")) {
            return "Barts Hospital Trust";

        } else {
            return "<UNKNOWN ORGANISATION>";
        }
    }
}
