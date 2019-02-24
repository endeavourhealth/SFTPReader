package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.endeavourhealth.sftpreader.model.db.EmisOrganisationMap;

import java.util.List;

public class EmisOrganisationHelper extends SftpOrganisationHelper {

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) throws Exception {
        return getOrganisationNameFromOdsCode(db, odsCode);
    }

    public static String getOrganisationNameFromOdsCode(DataLayerI db, String odsCode) throws Exception {
        //we can end up with multiple records for an ODS code, so just use the longer name we find
        String ret = null;
        List<EmisOrganisationMap> mappings = db.getEmisOrganisationMapsForOdsCode(odsCode);
        for (EmisOrganisationMap mapping: mappings) {
            String s = mapping.getName();
            if (ret == null
                    || s.length() > ret.length()) {
                ret = s;
            }
        }

        return ret;
    }
}
