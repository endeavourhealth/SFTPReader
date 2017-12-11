package org.endeavourhealth.sftpreader.implementations.emis;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.*;
import java.nio.charset.Charset;
import java.time.LocalDateTime;
import java.util.*;

public class EmisSftpBatchValidator extends SftpBatchValidator {

    @Override
    public void validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayer db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        checkExtractDateTimesIncrementBetweenBatches(incompleteBatch, lastCompleteBatch);

        checkFilenamesAreConsistentAcrossBatch(incompleteBatch, dbConfiguration);
        checkAllFilesArePresentInBatch(incompleteBatch, dbConfiguration);

        //check the newly received sharing agreements file to see if any orgs have become deleted or deactivated since last time
        checkForDeactivatedOrDeletedOrganisations(incompleteBatch, lastCompleteBatch, instanceConfiguration, dbConfiguration, db);

        // further checks to complete
        //
        // check that remote bytes == downloaded bytes
        // check all file attributes are complete
    }

    /**
     * checks the newly received sharing agreements file to see if any orgs have become deleted or deactivated since last time
     */
    private void checkForDeactivatedOrDeletedOrganisations(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration,
                                                           DbConfiguration dbConfiguration, DataLayer db) throws SftpValidationException {

        //if this is the first extract, then this will be null
        if (lastCompleteBatch == null) {
            return;
        }

        String sharingAgreementFileOld = EmisSftpBatchSplitter.findSharingAgreementsFile(instanceConfiguration, dbConfiguration, lastCompleteBatch);

        Map<String, String> hmActivatedOld = new HashMap<>();
        Map<String, String> hmDisabledOld = new HashMap<>();
        Map<String, String> hmDeletedOld = new HashMap<>();
        readSharingAgreementsFile(sharingAgreementFileOld, hmActivatedOld, hmDisabledOld, hmDeletedOld);

        String sharingAgreementFileNew = EmisSftpBatchSplitter.findSharingAgreementsFile(instanceConfiguration, dbConfiguration, incompleteBatch);
        
        Map<String, String> hmActivatedNew = new HashMap<>();
        Map<String, String> hmDisabledNew = new HashMap<>();
        Map<String, String> hmDeletedNew = new HashMap<>();
        readSharingAgreementsFile(sharingAgreementFileNew, hmActivatedNew, hmDisabledNew, hmDeletedNew);

        //check for changes in orgs that are in the file now, compared to before
        for (String orgGuid: hmActivatedNew.keySet()) {
            String activatedNew = hmActivatedNew.get(orgGuid);
            String disabledNew = hmDisabledNew.get(orgGuid);
            String deletedNew = hmDeletedNew.get(orgGuid);

            String activatedOld = hmActivatedOld.get(orgGuid);
            String disabledOld = hmDisabledOld.get(orgGuid);
            String deletedOld = hmDeletedOld.get(orgGuid);

            List<String> msgs = new ArrayList<>();

            if (activatedOld != null
                    && !activatedOld.equals(activatedNew)) {
                msgs.add("'activated' changed " + activatedOld + " -> " + activatedNew);
            }

            if (disabledOld != null
                    && !disabledOld.equals(disabledNew)) {
                msgs.add("'disabled' changed " + disabledOld + " -> " + disabledNew);
            }

            if (deletedOld != null
                    && !deletedOld.equals(deletedNew)) {
                msgs.add("'deleted' changed " + deletedOld + " -> " + deletedNew);
            }

            if (msgs.size() > 0) {
                String msg = String.join("\r\n", msgs);
                sendSlackAlert(orgGuid, msg, db);
            }
        }

        //check for orgs that were in the file but aren't any more
        for (String orgGuid: hmActivatedOld.keySet()) {
            if (!hmActivatedNew.containsKey(orgGuid)) {
                sendSlackAlert(orgGuid, "Has been removed from the sharing agreements file", db);
            }
        }
    }

    private static void sendSlackAlert(String emisOrgGuid, String msg, DataLayer db) throws SftpValidationException {

        //need to find the org details for the GUID
        EmisOrganisationMap mapping = null;
        try {
            mapping = db.getEmisOrganisationMap(emisOrgGuid);
        } catch (PgStoredProcException ex) {
            throw new SftpValidationException("Failed to access database", ex);
        }

        if (mapping == null) {
            throw new SftpValidationException("Failed to look up Org GUID " + emisOrgGuid);
        }

        String alert = "Sharing agreement for ";
        alert += mapping.getOdsCode();
        alert += ", ";
        alert += mapping.getName();
        alert += " has changed:\r\n";
        alert += msg;

        SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, alert);
    }

    private static void readSharingAgreementsFile(String filePath,
                                                  Map<String, String> hmActivated,
                                                  Map<String, String> hmDisabled,
                                                  Map<String, String> hmDeleted) throws SftpValidationException {

        CSVParser csvParser = null;
        try {
            InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(filePath);
            csvParser = new CSVParser(isr, EmisSftpBatchSplitter.CSV_FORMAT.withHeader());

            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                String orgGuid = csvRecord.get("OrganisationGuid");
                String activated = csvRecord.get("IsActivated");
                String disabled = csvRecord.get("Disabled");
                String deleted = csvRecord.get("Deleted");

                hmActivated.put(orgGuid, activated);
                hmDisabled.put(orgGuid, disabled);
                hmDeleted.put(orgGuid, deleted);
            }
        } catch (Exception ex) {
            throw new SftpValidationException("Failed to read sharing agreements file " + filePath, ex);

        } finally {
            try {
                if (csvParser != null) {
                    csvParser.close();
                }
            } catch (IOException ioe) {
                //if we fail to close, then it doesn't matter
            }
        }
    }

    private void checkExtractDateTimesIncrementBetweenBatches(Batch incompleteBatch, Batch lastCompleteBatch) throws SftpValidationException {

        if (lastCompleteBatch == null) {
            return;
        }

        LocalDateTime firstIncompleteBatchDatetime = EmisSftpFilenameParser.parseBatchIdentifier(incompleteBatch.getBatchIdentifier());
        LocalDateTime lastCompleteBatchDateTime = EmisSftpFilenameParser.parseBatchIdentifier(lastCompleteBatch.getBatchIdentifier());

        if ((firstIncompleteBatchDatetime.isBefore(lastCompleteBatchDateTime))
                || (firstIncompleteBatchDatetime.isEqual(lastCompleteBatchDateTime))) {

            throw new SftpValidationException("First unsequenced batch extract date time is before last sequenced batch extract date time.");
        }
    }

    private void checkFilenamesAreConsistentAcrossBatch(Batch incompleteBatches, DbConfiguration dbConfiguration) throws SftpValidationException {
        Validate.notNull(incompleteBatches, "incompleteBatches is null");
        Validate.notNull(incompleteBatches.getBatchFiles(), "incompleteBatches.batchFiles is null");

        Integer processingIdStart = null;
        Integer processingIdEnd = null;
        LocalDateTime extractDateTime = null;

        if (incompleteBatches.getBatchFiles().size() == 0)
            throw new SftpValidationException("No batch files in batch");

        boolean first = true;

        for (BatchFile incompleteBatchFile : incompleteBatches.getBatchFiles()) {
            EmisSftpFilenameParser emisSftpFilenameParser = new EmisSftpFilenameParser(incompleteBatchFile.getFilename(), dbConfiguration);

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

            for (BatchFile incompleteBatchFile : incompleteBatch.getBatchFiles())
                if (fileType.equals(incompleteBatchFile.getFileTypeIdentifier()))
                    found = true;

            if (!found)
                throw new SftpValidationException("Could not find file of type " + fileType + " in batch.  Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

        if (incompleteBatch.getBatchFiles().size() != dbConfiguration.getInterfaceFileTypes().size())
            throw new SftpValidationException("Incorrect number of files in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
    }


}
