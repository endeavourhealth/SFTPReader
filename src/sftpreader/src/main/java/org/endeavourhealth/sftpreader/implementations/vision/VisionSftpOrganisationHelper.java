package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.common.ods.OdsOrganisation;
import org.endeavourhealth.common.ods.OdsWebService;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpOrganisationHelper;
import org.slf4j.LoggerFactory;

import java.net.Proxy;

public class VisionSftpOrganisationHelper extends SftpOrganisationHelper {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(VisionSftpOrganisationHelper.class);

    @Override
    public String findOrganisationNameFromOdsCode(DataLayerI db, String odsCode) {

        try {
            //SFTP Reader instance needs a proxy to connect to the internet, so get it from the SlackHelper
            //which has the proxy settings in its config JSON
            Proxy proxy = SlackHelper.getProxy();
            OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(odsCode, proxy);
            //OdsOrganisation odsOrg = OdsWebService.lookupOrganisationViaRest(odsCode);
            if (odsOrg != null) {
                return odsOrg.getOrganisationName();

            } else {
                return null;
            }

        } catch (Exception ex) {
            LOG.error("Error looking up org ID [" + odsCode + "]", ex);
            return "ERROR_IN_LOOKUP";
            //return null;
        }
    }
}
