package org.endeavourhealth.sftpreader.implementations.bhrut;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

public class BhrutOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {
        if (odsCode.equalsIgnoreCase("RF4")) {
            return "Barking, Havering and Redbridge University Hospitals NHS Trust";

        } else {
            return "Unexpected Organisation ODS code used for BHRUT service: "+odsCode;
        }
    }
}
