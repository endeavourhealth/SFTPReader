package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisFixDisabledService;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.implementations.emis.utility.SharingAgreementRecord;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class EmisPostSplitBatchValidator extends SftpPostSplitBatchValidator {
    private static final Logger LOG = LoggerFactory.getLogger(EmisPostSplitBatchValidator.class);

    @Override
    public void validateBatchPostSplit(Batch newBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {

        List<BatchSplit> splits = db.getBatchSplitsForBatch(newBatch.getBatchId());
        LOG.trace("Found " + splits.size() + " splits for batch " + newBatch.getBatchId());

        for (BatchSplit split: splits) {

            String odsCode = split.getOrganisationId();


            //now we've split the files, we can attempt to fix any disabled extract
            attemptDisabledExtractFixIfNecessary(odsCode, newBatch, lastCompleteBatch, db, instanceConfiguration, dbConfiguration);

        }
    }


    private void attemptDisabledExtractFixIfNecessary(String odsCode, Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) {

        //work out if this feed was disabled and is now enabled again
        if (lastCompleteBatch == null) {
            return;
        }

        try {
            //the new sharing agreement file will exist in temp dir, so read from there
            String newSharingAgreementFile = EmisHelper.findPreSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, EmisConstants.SHARING_AGREEMENTS_FILE_TYPE);
            Map<String, SharingAgreementRecord> hmNew = SharingAgreementRecord.readSharingAgreementsFile(newSharingAgreementFile);

            //find the new sharing state, handling the fact we may have multiple records for the org ODS code in our mapping table
            EmisOrganisationMap org = null;
            SharingAgreementRecord newSharingState = null;

            List<EmisOrganisationMap> possibleOrgs = db.getEmisOrganisationMapsForOdsCode(odsCode);
            for (EmisOrganisationMap possibleOrg: possibleOrgs) {
                String possibleGuid = possibleOrg.getGuid();
                SharingAgreementRecord state = hmNew.get(possibleGuid);
                if (state != null) {
                    org = possibleOrg;
                    newSharingState = state;
                }
            }

            String orgGuid = org.getGuid();

            /*LOG.trace("org = " + org);
            LOG.trace("New sharing file is " + newSharingAgreementFile + " exists = " + new File(newSharingAgreementFile).exists());
            LOG.trace("HmNew size = " + hmNew.size());
            for (String key: hmNew.keySet()) {
                SharingAgreementRecord val = hmNew.get(key);
                LOG.trace("Got key [" + key + "] value [" + val + "]");
            }
            LOG.trace("Got new state " + newSharingState + " for key [" + orgGuid + "]");*/

            if (newSharingState.isDisabled()) {
                //if still disabled, return out
                return;
            }

            //the previous sharing agreement file will no longer exist in temp, so we need to read it from permanent storage
            try {
                String lastSharingAgreementFile = EmisHelper.findPostSplitFileInPermanentDir(db, instanceConfiguration, dbConfiguration, lastCompleteBatch, odsCode, EmisConstants.SHARING_AGREEMENTS_FILE_TYPE);
                Map<String, SharingAgreementRecord> hmOld = SharingAgreementRecord.readSharingAgreementsFile(lastSharingAgreementFile);
                SharingAgreementRecord oldSharingState = hmOld.get(orgGuid);
                if (!oldSharingState.isDisabled()) {
                    //if not previously disabled, return out
                    return;
                }
            } catch (Exception ex2) {
                //if we specifically get an exception saying there isn't a previous file for this org, then that's fine
                //and will happen the first time we get date for a service
                String msg = ex2.getMessage();
                if (msg.startsWith("Failed to find batch split")) {
                    return;
                } else {
                    //if something else, throw up
                    throw ex2;
                }
            }

            LOG.trace("Looks like " + org.getOdsCode() + " " + org.getName() + " was disabled and is now fixed");

            //take this out so we don't make things worse until we understand what Emis have changed
            //if our feed was disabled, but is now fixed, then we should try to fix the files so we don't need to
            //process the full delete before the re-bulk
            EmisFixDisabledService fixer = new EmisFixDisabledService(org, db, instanceConfiguration, dbConfiguration);
            fixer.fixDisabledExtract();

            //send slack notification
            String msg = "Disabled extract files for " + org.getOdsCode() + " " + org.getName()
                    + " have been automatically fixed, and can now be re-queued into Inbound queue";
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

        } catch (Exception ex) {
            throw new RuntimeException("Error fixing disabled feed for " + odsCode, ex);
        }
    }
}
