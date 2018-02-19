package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;

public class VisionSftpOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) {

        try {
            //TODO: Stu implementing odsCode lookup

            //OrganisationEntity org = OrganisationEntity.getOrganisationByodsCode(odsCode);
            //return org.getName();

            return odsCode;
        }
        catch (Exception Ex) {
            return null;
        }
    }
}
