package org.endeavourhealth.sftpreader.implementations.emis;

import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.SftpPostSplitBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
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

            //the previous sharing agreement file will no longer exist in temp, so we need to read it from permanent storage
            String lastSharingAgreementFile = EmisHelper.findSharingAgreementsFileInPermanentDir(db, instanceConfiguration, dbConfiguration, lastCompleteBatch, odsCode);
            Map<String, SharingAgreementRecord> hmOld = EmisHelper.readSharingAgreementsFile(lastSharingAgreementFile);
            SharingAgreementRecord oldSharingState = hmOld.get(orgGuid);
            if (oldSharingState == null
                    || !oldSharingState.isDisabled()) {
                //if not previously disabled, return out
                return;
            }

            String newSharingAgreementFile = EmisHelper.findSharingAgreementsFileInTempDir(instanceConfiguration, dbConfiguration, batch);
            Map<String, SharingAgreementRecord> hmNew = EmisHelper.readSharingAgreementsFile(newSharingAgreementFile);
            SharingAgreementRecord newSharingState = hmNew.get(orgGuid);
            if (newSharingState.isDisabled()) {
                //if still disabled, return out
                return;
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
