package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.implementations.emis.utility.SharingAgreementRecord;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

public class EmisBatchValidator extends SftpBatchValidator {
    private static final Logger LOG = LoggerFactory.getLogger(EmisBatchValidator.class);

    private static final String SHARING_AGREEMENT_UUID_KEY = "SharingAgreementGuid";

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        checkExtractDateTimesIncrementBetweenBatches(incompleteBatch, lastCompleteBatch);

        checkFilenamesAreConsistentAcrossBatch(incompleteBatch, dbConfiguration);
        checkAllFilesArePresentInBatch(incompleteBatch, dbConfiguration);
        checkSharingAgreementIdIsCorrect(incompleteBatch, dbConfiguration);

        //check the newly received sharing agreements file to see if any orgs have become deleted or deactivated since last time
        checkForDeactivatedOrDeletedOrganisations(incompleteBatch, lastCompleteBatch, instanceConfiguration, dbConfiguration, db);

        return true;
    }

    private void checkSharingAgreementIdIsCorrect(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {

        //find the expected sharing agreement from the DB.
        UUID expectedSharingAgreement = null;
        for (DbConfigurationKvp dbConfigurationKvp : dbConfiguration.getDbConfigurationKvp()) {
            if (dbConfigurationKvp.getKey().equals(SHARING_AGREEMENT_UUID_KEY)) {
                expectedSharingAgreement = UUID.fromString(dbConfigurationKvp.getValue());
                break;
            }
        }
        if (expectedSharingAgreement == null) {
            throw new SftpValidationException(SHARING_AGREEMENT_UUID_KEY + " has not been configured in configuration_kvp table");
        }

        //find the sharing agreement from the files, doing each file (just in case)
        for (BatchFile file: incompleteBatch.getBatchFiles()) {
            String fileName = file.getFilename();

            //the file name is in the format
            //291_Agreements_SharingOrganisation_20150211164536_45E7CD20-EE37-41AB-90D6-DC9D4B03D102.csv.gpg
            //so we need to remove BOTH extensions
            fileName = FilenameUtils.getBaseName(fileName);
            fileName = FilenameUtils.getBaseName(fileName);

            String[] toks = fileName.split("_");
            String agreementStr = toks[4];
            UUID foundSharingAgreement = UUID.fromString(agreementStr);
            if (!foundSharingAgreement.equals(expectedSharingAgreement)) {
                throw new SftpValidationException("Sharing agreement " + foundSharingAgreement + " does not match exepected UUID " + expectedSharingAgreement);
            }
        }
    }


    /**
     * checks the newly received sharing agreements file to see if any orgs have become deleted or deactivated since last time
     */
    private void checkForDeactivatedOrDeletedOrganisations(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration,
                                                           DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        //if this is the first extract, then this will be null
        if (lastCompleteBatch == null) {
            return;
        }

        Map<String, SharingAgreementRecord> hmOld = readOldSharingAgreementFiles(instanceConfiguration, dbConfiguration, lastCompleteBatch);

        String sharingAgreementFileNew = EmisHelper.findPreSplitFileInTempDir(instanceConfiguration, dbConfiguration, incompleteBatch, EmisConstants.SHARING_AGREEMENTS_FILE_TYPE);
        //LOG.debug("Reading NEW sharing agreement file " + sharingAgreementFileNew);
        
        Map<String, SharingAgreementRecord> hmNew = SharingAgreementRecord.readSharingAgreementsFile(sharingAgreementFileNew);

        //check for changes in orgs that are in the file now, compared to before
        for (String orgGuid: hmNew.keySet()) {
            SharingAgreementRecord newRecord = hmNew.get(orgGuid);
            SharingAgreementRecord oldRecord = hmOld.get(orgGuid);

            List<String> msgs = new ArrayList<>();

            if (oldRecord != null && oldRecord.isActivated() != newRecord.isActivated()) {
                msgs.add("'activated' changed " + oldRecord.isActivated() + " -> " + newRecord.isActivated());
            }

            if (oldRecord != null && oldRecord.isDisabled() != newRecord.isDisabled()) {
                msgs.add("'disabled' changed " + oldRecord.isDisabled() + " -> " + newRecord.isDisabled());
            }

            if (oldRecord != null && oldRecord.isDeleted() != newRecord.isDeleted()) {
                msgs.add("'deleted' changed " + oldRecord.isDeleted() + " -> " + newRecord.isDeleted());
            }

            if (msgs.size() > 0) {
                String msg = String.join("\r\n", msgs);
                sendSlackAlert(orgGuid, msg, db, dbConfiguration);
            }
        }

        //check for orgs that were in the file but aren't any more
        for (String orgGuid: hmOld.keySet()) {
            if (!hmNew.containsKey(orgGuid)) {
                sendSlackAlert(orgGuid, "Has been removed from the sharing agreements file", db, dbConfiguration);
            }
        }
    }

    /**
     * the previous sharing agreement file will have been split over all the org directories, so we need
     * to read them all to recreate it as a whole
     */
    private Map<String, SharingAgreementRecord> readOldSharingAgreementFiles(DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration,
                                              Batch lastCompleteBatch) throws SftpValidationException {

        Map<String, SharingAgreementRecord> ret = new HashMap<>();
        
        //all the split files will have the same file name, which we can find from the batch file list
        String fileName = null;
        for (BatchFile batchFile: lastCompleteBatch.getBatchFiles()) {
            if (batchFile.getFileTypeIdentifier().equalsIgnoreCase(EmisConstants.SHARING_AGREEMENTS_FILE_TYPE)) {
                fileName = EmisFilenameParser.getDecryptedFileName(batchFile, dbConfiguration);
            }
        }

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = lastCompleteBatch.getLocalRelativePath();
        String splitDir = EmisBatchSplitter.SPLIT_FOLDER;

        String dir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        dir = FilenameUtils.concat(dir, batchDir);
        dir = FilenameUtils.concat(dir, splitDir);

        List<String> files = null;
        try {
            files = FileHelper.listFilesInSharedStorage(dir);

        } catch (Exception ex) {
            throw new SftpValidationException(ex);
        }

        for (String filePath: files) {

            String splitFileName = FilenameUtils.getName(filePath);
            if (splitFileName.equalsIgnoreCase(fileName)) {
                //LOG.debug("Reading OLD sharing agreement file " + filePath);

                Map<String, SharingAgreementRecord> splitFileContents = SharingAgreementRecord.readSharingAgreementsFile(filePath);
                for (String orgGuid: splitFileContents.keySet()) {
                    if (ret.containsKey(orgGuid)) {
                        throw new SftpValidationException("Found org GUID " + orgGuid + " in multiple split sub-dirs");
                    }

                    SharingAgreementRecord r = splitFileContents.get(orgGuid);
                    ret.put(orgGuid, r);
                }
            }
        }
        
        return ret;
    }

    private static EmisOrganisationMap findOrgDetails(DataLayerI db, String emisOrgGuid) throws SftpValidationException {

        EmisOrganisationMap mapping = null;
        try {
            mapping = db.getEmisOrganisationMap(emisOrgGuid);
        } catch (Exception ex) {
            throw new SftpValidationException("Failed to access database", ex);
        }

        if (mapping == null) {
            throw new SftpValidationException("Failed to look up Org GUID " + emisOrgGuid);
        }

        return mapping;
    }

    private static void sendSlackAlert(String emisOrgGuid, String msg, DataLayerI db, DbConfiguration dbConfiguration) throws SftpValidationException {

        //need to find the org details for the GUID
        EmisOrganisationMap mapping = findOrgDetails(db, emisOrgGuid);

        String alert = "Sharing agreement for ";
        alert += mapping.getOdsCode();
        alert += ", ";
        alert += mapping.getName();
        alert += " (";
        alert += dbConfiguration.getConfigurationFriendlyName();
        alert += ") has changed:\r\n";
        alert += msg;

        alert += "\r\n";
        alert += dbConfiguration.getConfigurationFriendlyName(); //this contains the sharing agreement name

        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, alert);
    }


    private void checkExtractDateTimesIncrementBetweenBatches(Batch incompleteBatch, Batch lastCompleteBatch) throws SftpValidationException {

        if (lastCompleteBatch == null) {
            return;
        }

        LocalDateTime firstIncompleteBatchDatetime = EmisFilenameParser.parseBatchIdentifier(incompleteBatch.getBatchIdentifier());
        LocalDateTime lastCompleteBatchDateTime = EmisFilenameParser.parseBatchIdentifier(lastCompleteBatch.getBatchIdentifier());

        if ((firstIncompleteBatchDatetime.isBefore(lastCompleteBatchDateTime))
                || (firstIncompleteBatchDatetime.isEqual(lastCompleteBatchDateTime))) {

            throw new SftpValidationException("First unsequenced batch extract date time is before last sequenced batch extract date time.");
        }
    }

    private void checkFilenamesAreConsistentAcrossBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
        Validate.notNull(incompleteBatch, "incompleteBatches is null");
        Validate.notNull(incompleteBatch.getBatchFiles(), "incompleteBatches.batchFiles is null");

        Integer processingIdStart = null;
        Integer processingIdEnd = null;
        LocalDateTime extractDateTime = null;

        if (incompleteBatch.getBatchFiles().size() == 0)
            throw new SftpValidationException("No batch files in batch");

        boolean first = true;

        for (BatchFile incompleteBatchFile : incompleteBatch.getBatchFiles()) {

            RemoteFile remoteFile = new RemoteFile(incompleteBatchFile.getFilename(), -1, null);
            EmisFilenameParser emisSftpFilenameParser = new EmisFilenameParser(false, remoteFile, dbConfiguration);

            if (first) {
                processingIdStart = emisSftpFilenameParser.getProcessingIds().getProcessingIdStart();
                processingIdEnd = emisSftpFilenameParser.getProcessingIds().getProcessingIdEnd();
                extractDateTime = emisSftpFilenameParser.getExtractDateTime();

                first = false;
            } else {
                if (emisSftpFilenameParser.getProcessingIds().getProcessingIdStart() != processingIdStart)
                    throw new SftpValidationException("Emis start processing id does not match the rest in the batch.  Filename = " + incompleteBatchFile.getFilename());

                if (emisSftpFilenameParser.getProcessingIds().getProcessingIdEnd() != processingIdEnd)
                    throw new SftpValidationException("Emis end processing id does not match the rest in the batch.  Filename = " + incompleteBatchFile.getFilename());

                if (!emisSftpFilenameParser.getExtractDateTime().equals(extractDateTime))
                    throw new SftpValidationException("Emis extract date time does not match the rest in the batch.  Filename = " + incompleteBatchFile.getFilename());
            }
        }
    }

    private void checkAllFilesArePresentInBatch(Batch incompleteBatch, DbConfiguration dbConfiguration) throws SftpValidationException {
        for (String fileType : dbConfiguration.getInterfaceFileTypes()) {
            boolean found = false;

            for (BatchFile incompleteBatchFile : incompleteBatch.getBatchFiles()) {
                if (fileType.equals(incompleteBatchFile.getFileTypeIdentifier())) {
                    found = true;
                }
            }

            if (!found) {
                throw new SftpValidationException("Could not find file of type " + fileType + " in batch.  Batch identifier = " + incompleteBatch.getBatchIdentifier());
            }
        }

        if (incompleteBatch.getBatchFiles().size() != dbConfiguration.getInterfaceFileTypes().size()) {
            throw new SftpValidationException("Incorrect number of files in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

    }


}
