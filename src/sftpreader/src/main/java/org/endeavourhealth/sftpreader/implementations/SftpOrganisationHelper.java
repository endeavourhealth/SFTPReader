package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.model.DataLayerI;


public abstract class SftpOrganisationHelper {

    public abstract String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) throws Exception;
}
