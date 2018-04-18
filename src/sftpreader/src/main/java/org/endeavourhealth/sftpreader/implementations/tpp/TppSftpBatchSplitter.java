package org.endeavourhealth.sftpreader.implementations.tpp;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class TppSftpBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(TppSftpBatchSplitter.class);

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);
    private static final String SPLIT_COLUMN_ORG = "IDOrganisationVisibleTo";
    private static final String SPLIT_FOLDER = "Split";
    private static final String ORGANISATION_FILE = "SROrganisation.csv";

    private static Set<String> cachedFilesToIgnore = null;
    private static Set<String> cachedFilesToSplit = null;
    private static Set<String> cachedFilesToNotSplit = null;

    /**
     * splits the TPP extract files org ID, storing the results in sub-directories using that org ID as the name
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the big CSV files should already be in our temp storage. If so, use those files rather than the ones from permanent storage
        String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        sourceTempDir = FilenameUtils.concat(sourceTempDir, batchDir);

        String sourcePermDir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        sourcePermDir = FilenameUtils.concat(sourcePermDir, batchDir);

        String splitTempDir = FilenameUtils.concat(tempDir, configurationDir);
        splitTempDir = FilenameUtils.concat(splitTempDir, batchDir);
        splitTempDir = FilenameUtils.concat(splitTempDir, SPLIT_FOLDER);

        LOG.trace("Splitting CSV files to " + splitTempDir);

        File dstDir = new File(splitTempDir);

        //if the folder does exist, delete all content within it, since if we're re-splitting a file
        //we want to make sure that all previous content is deleted
        FileHelper.deleteRecursiveIfExists(dstDir);
        FileHelper.createDirectoryIfNotExists(dstDir);

        //work out which files we want to split
        List<File> filesToSplit = new ArrayList<>();
        List<File> filesToNotSplit = new ArrayList<>();
        identifyFiles(sourceTempDir, filesToSplit, filesToNotSplit);

        //split the files we can
        for (File f: filesToSplit) {
            splitFile(f.getAbsolutePath(), dstDir, CSV_FORMAT, SPLIT_COLUMN_ORG);
        }

        List<File> orgDirs = new ArrayList<>();
        for (File orgDir: dstDir.listFiles()) {
            orgDirs.add(orgDir);
        }
        //LOG.info("Got org dirs = " + dstDir.listFiles().length);

        //before we copy the non-split files into the org directories, we need to make sure
        //to account for any organisations that ARE extracted but just so happen to have no data in this instance
        //by making sure we have an org directory for every org we had in our previous batch
        Batch lastCompleteBatch = db.getLastCompleteBatch(dbConfiguration.getConfigurationId());
        if (lastCompleteBatch != null) {
            List<BatchSplit> lastCompleteBatchSplits = db.getBatchSplitsForBatch(lastCompleteBatch.getBatchId());
            for (BatchSplit previousBatchSplit: lastCompleteBatchSplits) {
                String localRelativePath = previousBatchSplit.getLocalRelativePath();
                String orgId = new File(localRelativePath).getName();

                String orgDir = FilenameUtils.concat(splitTempDir, orgId);
                FileHelper.createDirectoryIfNotExists(orgDir);

                File orgDirFile = new File(orgDir);
                if (!orgDirs.contains(orgDirFile)) {
                    orgDirs.add(orgDirFile);
                }
            }
        }

        //copy the non-splitting files into each of the org directories
        for (File f: filesToNotSplit) {
            for (File orgDir: orgDirs) {
                File dst = new File(orgDir, f.getName());
                Files.copy(f.toPath(), dst.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        //we need to parse the organisation file, to store the mappings for later
        saveAllOdsCodes(sourceTempDir, db, instanceConfiguration, dbConfiguration, batch);

        LOG.trace("Completed CSV file splitting to " + dstDir);

        //copy all our files to permanent storage and create the batch split objects
        //build a list of the folders containing file sets, to return
        List<BatchSplit> ret = new ArrayList<>();

        for (File orgDir : orgDirs) {

            String orgId = orgDir.getName();
            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, orgId);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(orgId); //the TPP org ID IS the ODS Code

            ret.add(batchSplit);

            //if we're using separate temp storage to our permanent storage, then copy everything to it
            if (!FilenameUtils.equals(tempDir, sharedStorageDir)) {

                String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
                storagePath = FilenameUtils.concat(storagePath, orgId);

                File[] splitFiles = orgDir.listFiles();
                LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage");

                for (File splitFile: splitFiles) {

                    String fileName = splitFile.getName();
                    String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                    FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
                }
            }
        }

        return ret;
    }

    private void identifyFiles(String sourceTempDir, List<File> filesToSplit, List<File> filesToNotSplit) throws Exception {
        for (File tempFile: new File(sourceTempDir).listFiles()) {

            //the "Split" sub-directory will be there, so ignore it
            if (tempFile.isDirectory()) {
                continue;
            }

            String name = tempFile.getName();

            //and we'll also have non-CSV files (the zip and fragments), so ignore them too
            String ext = FilenameUtils.getExtension(name);
            if (!ext.equalsIgnoreCase("csv")) {
                continue;
            }

            name = FilenameUtils.getBaseName(name);

            //NOTE - there are some files that DO contain an organisation ID column but shouldn't be split (e.g. SRCtv3Hierarchy),
            //so we need these explicit lists of how to handle each file, rather than being able to work it out dynamically
            if (getFilesToIgnore().contains(name)) {
                //ignore it

            } else if (getFilesToSplit().contains(name)) {
                filesToSplit.add(tempFile);

            } else if (getFilesToNotSplit().contains(name)) {
                filesToNotSplit.add(tempFile);

            } else {
                throw new Exception("Unknown file " + tempFile);
            }
        }
    }

    private static Set<String> getFilesToIgnore() {
        if (cachedFilesToIgnore == null) {
            Set<String> set = new HashSet<>();

            set.add("SRManifest");

            cachedFilesToIgnore = set;
        }
        return cachedFilesToIgnore;
    }

    private static Set<String> getFilesToNotSplit() {
        if (cachedFilesToNotSplit == null) {
            Set<String> set = new HashSet<>();

            set.add("SRCcg");
            set.add("SRCtv3");
            set.add("SRMapping");
            set.add("SRMappingGroup");
            set.add("SRConfiguredListOption");
            set.add("SRCtv3ToVersion2");
            set.add("SRCtv3Hierarchy");
            set.add("SRImmunisationContent");
            set.add("SRMedicationReadCodeDetails");
            set.add("SROrganisation");
            set.add("SROrganisationBranch");
            set.add("SRStaff");
            set.add("SRStaffMemberProfile");
            set.add("SRStaffMember");
            set.add("SRStaffMemberProfileRole");
            set.add("SRTrust");

            cachedFilesToNotSplit = set;
        }
        return cachedFilesToNotSplit;
    }

    private static Set<String> getFilesToSplit() {
        if (cachedFilesToSplit == null) {
            Set<String> set = new HashSet<>();

            set.add("SRSection");
            set.add("SRTheatreBooking");
            set.add("SRSectionedBy");
            set.add("SROohNewCase");
            set.add("SRSectionRecall");
            set.add("SRReferralAllocationStaff");
            set.add("SRHospitalWaitingList");
            set.add("SRChildHealthGroupAction");
            set.add("SRRttStatus");
            set.add("SRImmunisationConsent");
            set.add("SRSmsConsent");
            set.add("SREvent");
            set.add("SRSchoolHistory");
            set.add("SRRefusedOffer");
            set.add("SRAAndEAttendance");
            set.add("SRPatientLeave");
            set.add("SRVisit");
            set.add("SRProblem");
            set.add("SRGPPracticeHistory");
            set.add("SRRttPause");
            set.add("SRReferralIn");
            set.add("SRAppointment");
            set.add("SRCarePlanReview");
            set.add("SRCarePlanDetail");
            set.add("SRStaffSpecialty");
            set.add("SROutPatientMedication");
            set.add("SRRecordStatus");
            set.add("SRTheatreSessionAttendees");
            set.add("SRHospitalConsultantEvent");
            set.add("SRMHConsent");
            set.add("SRClinicalCode");
            set.add("SREventLink");
            set.add("SRMentalHealthAssessment");
            set.add("SRBedLocation");
            set.add("SRTeamMember");
            set.add("SRPatientGroups");
            set.add("SRHospitalAAndECode");
            set.add("SRECTCourse");
            set.add("SRMHEpisode");
            set.add("SRTreatmentCentreExclusions");
            set.add("SRHospitalPatientAlertIndicator");
            set.add("SRAppointmentVisitOutcomes");
            set.add("SRRtt");
            set.add("SRAssessmentOutcome");
            set.add("SRCarePlanItem");
            set.add("SR18WeekWait");
            set.add("SRTeam");
            set.add("SROohTransport");
            set.add("SRTheatreSession");
            set.add("SRHospitalWaitingListSuspension");
            set.add("SROohPathways");
            set.add("SRSystmOnline");
            set.add("SRHospitalAAndENumber");
            set.add("SRPatientInformation");
            set.add("SRTreatmentCentreSession");
            set.add("SRMedia");
            set.add("SRWardStay");
            set.add("SRHospitalWaitingListIntendedProcedure");
            set.add("SRCaseloadHistory");
            set.add("SROohAssessment");
            set.add("SRAAndEEvent");
            set.add("SRBedPatient");
            set.add("SRNeedsLinkage");
            set.add("SRActivityEvent");
            set.add("SRStaffActivity");
            set.add("SRBedClosure");
            set.add("SRChildAtRisk");
            set.add("SRHospitalWardBooking");
            set.add("SRPatientContactDetails");
            set.add("SRSchedulingSuspension");
            set.add("SRRecall");
            set.add("SRECTTreatment");
            set.add("SRService");
            set.add("SRScheduledEvent");
            set.add("SRSectionAppeal");
            set.add("SRDrugSensitivity");
            set.add("SRResponsibleParty");
            set.add("SRPrimaryCareMedication");
            set.add("SRTTOMedication");
            set.add("SRTreatmentCentreSessionTemporaryAmendments");
            set.add("SRTemplate");
            set.add("SRCode");
            set.add("SRLocation");
            set.add("SRPlaceHolderMedication");
            set.add("SRPatient");
            set.add("SRVariableDoseCDMedication");
            set.add("SROohThirdPartyCall");
            set.add("SRHospitalTransport");
            set.add("SRPatientAddressHistory");
            set.add("SRCarePlanPerformance");
            set.add("SRCarePlan");
            set.add("SRAssessment");
            set.add("SRDischargeDelay");
            set.add("SROohEmergencyCall");
            set.add("SRCarePlanFrequency");
            set.add("SRReferralOut");
            set.add("SRLocationAttribute");
            set.add("SRHospitalNoteTracking");
            set.add("SREquipment");
            set.add("SRReferralInIntervention");
            set.add("SRCarePlanSkillset");
            set.add("SRMHAWOL");
            set.add("SRRotaSlot");
            set.add("SRPatientContactProperty");
            set.add("SRNeeds");
            set.add("SRCluster");
            set.add("SROnlineServices");
            set.add("SRWaitingList");
            set.add("SRCaseload");
            set.add("SRAppointmentFlags");
            set.add("SRHospitalAdmissionAndDischarge");
            set.add("SRGoal");
            set.add("SROnlineUsers");
            set.add("SRRepeatTemplate");
            set.add("SRHospitalAlertIndicator");
            set.add("SROohAction");
            set.add("SRChildHealthGroupActionSchedulingParameters");
            set.add("SRQuestionnaire");
            set.add("SRCHSStatusHistory");
            set.add("SRCPA");
            set.add("SRAddressBookEntry");
            set.add("SRAppointmentRoom");
            set.add("SRReferralContactEvent");
            set.add("SRServiceDeliverySchedule");
            set.add("SRPatientRelationship");
            set.add("SRReferralAllocation");
            set.add("SROnAdmissionMedication");
            set.add("SRHospitalAdmissionReason");
            set.add("SRImmunisation");
            set.add("SRAssessmentReportedHealthCondition");
            set.add("SRTreatmentCentrePreference");
            set.add("SRPatientRegistration");
            set.add("SRReferralOutStatusDetails");
            set.add("SRExpense");
            set.add("SRRiskReview");
            set.add("SRPatientLocation");
            set.add("SRRota");
            set.add("SROohVisit");
            set.add("SRContacts");
            set.add("SRReferralInStatusDetails");
            set.add("SRSpecialNotes");
            set.add("SROohPassToPCC");
            set.add("SRSectionRightsExplained");
            set.add("SRAnsweredQuestionnaire");
            set.add("SRReferralInReferralReason");
            set.add("SROohCallBack");
            set.add("SRStaffSkillSet");
            set.add("SRProblemSubstance");
            set.add("SRHospitalDischargeLetter");
            set.add("SROohCaseOutcome");

            cachedFilesToSplit = set;
        }
        return cachedFilesToSplit;
    }

    private static void saveAllOdsCodes(String sourceTempDir, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws Exception {

        String orgFilePath = FilenameUtils.concat(sourceTempDir, ORGANISATION_FILE);
        File f = new File(orgFilePath);
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis);
        CSVParser csvParser = new CSVParser(reader, CSV_FORMAT.withHeader());

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                String orgName = csvRecord.get("Name");
                String orgOds = csvRecord.get("ID");

                if (!Strings.isNullOrEmpty(orgOds)) {
                    TppOrganisationMap mapping = new TppOrganisationMap();
                    mapping.setOdsCode(orgOds);
                    mapping.setName(orgName);

                    db.addTppOrganisationMap(mapping);
                }
            }
        } finally {
            csvParser.close();
        }
    }


    private static Set<File> splitFile(String sourceFilePath, File dstDir, CSVFormat csvFormat, String... splitColmumns) throws Exception {
        CsvSplitter csvSplitter = new CsvSplitter(sourceFilePath, dstDir, csvFormat, splitColmumns);
        return csvSplitter.go();
    }
}
