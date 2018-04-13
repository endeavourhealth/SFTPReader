package org.endeavourhealth.sftpreader.implementations;

import org.endeavourhealth.sftpreader.DataLayer;

public abstract class SftpOrganisationHelper {

    public abstract String findOrganisationNameFromOdsCode(DataLayer db, String odsCode) throws Exception;
}
