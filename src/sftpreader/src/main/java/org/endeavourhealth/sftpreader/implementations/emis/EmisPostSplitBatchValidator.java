package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
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
            EmisOrganisationMap org = db.getEmisOrganisationMapForOdsCode(odsCode);

            //now we've split the files, we can attempt to fix any disabled extract
            attemptDisabledExtractFixIfNecessary(org, newBatch, lastCompleteBatch, db, instanceConfiguration, dbConfiguration);

        }
    }


    private void attemptDisabledExtractFixIfNecessary(EmisOrganisationMap org, Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) {

        //work out if this feed was disabled and is now enabled again
        if (lastCompleteBatch == null) {
            return;
        }

        String orgGuid = org.getGuid();
        String odsCode = org.getOdsCode();


        try {
            //the new sharing agreement file will exist in temp dir, so read from there
            String newSharingAgreementFile = EmisHelper.findSharingAgreementsFileInTempDir(instanceConfiguration, dbConfiguration, batch);
            Map<String, SharingAgreementRecord> hmNew = EmisHelper.readSharingAgreementsFile(newSharingAgreementFile);
            SharingAgreementRecord newSharingState = hmNew.get(orgGuid);

            LOG.trace("New sharing file is " + newSharingAgreementFile + " exists = " + new File(newSharingAgreementFile).exists());
            LOG.trace("HmNew size = " + hmNew.size());
            for (String key: hmNew.keySet()) {
                SharingAgreementRecord val = hmNew.get(key);
                LOG.trace("Got key [" + key + "] value [" + val + "]");
            }
            LOG.trace("Got new state " + newSharingState + " for key [" + orgGuid + "]");

            if (newSharingState.isDisabled()) {
                //if still disabled, return out
                return;
            }
//TODO - need to make whatever fix for new against the old too!!
            //the previous sharing agreement file will no longer exist in temp, so we need to read it from permanent storage
            try {
                String lastSharingAgreementFile = EmisHelper.findSharingAgreementsFileInPermanentDir(db, instanceConfiguration, dbConfiguration, lastCompleteBatch, odsCode);
                Map<String, SharingAgreementRecord> hmOld = EmisHelper.readSharingAgreementsFile(lastSharingAgreementFile);
                SharingAgreementRecord oldSharingState = hmOld.get(orgGuid);
                if (oldSharingState == null
                        || !oldSharingState.isDisabled()) {
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

            //if our feed was disabled, but is now fixed, then we should try to fix the files so we don't need to
            //process the full delete before the re-bulk
            EmisFixDisabledService fixer = new EmisFixDisabledService(org, db, instanceConfiguration, dbConfiguration);
            fixer.fixDisabledExtract();

            //send slack notification
            String msg = "Disabled extract files for " + org.getOdsCode() + " " + org.getName()
                    + " have been automatically fixed, and can now be re-queued into Inbound queue";
            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, msg);

        } catch (Exception ex) {
            throw new RuntimeException("Error fixing disabled feed for " + org.getOdsCode(), ex);
        }
    }
}
