package org.endeavourhealth.sftpreader.implementations.adastra;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvJoiner;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class AdastraBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(AdastraBatchSplitter.class);

    private static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withDelimiter('|');

    private static final String SPLIT_FOLDER = "Split";
    private static final String FILE_TYPE_CASE = "CASE";
    private static final String FILE_TYPE_USERS = "USERS";

    /**
     * split Adastra files by the source ODS code. The ODS code is only present in the CASE file so we do that
     * and use the results of that to guide the remaining files.
     * The USERS file just gets duplicated for each service.
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

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

        //find known ODS codes previously in extracts for this configuration
        Set<String> expectedOdsCodes = db.getAdastraOdsCodes(dbConfiguration.getConfigurationId());

        //find the case file
        String caseFilePath = findCaseFile(batch, sourcePermDir, dbConfiguration);

        //read case file to work out the case ref -> ODS code mapping
        Map<String, String> hmCaseToOds = parseCaseFile(caseFilePath);

        //split case file by ODS code
        CsvSplitter csvSplitter = new CsvSplitter(caseFilePath, dstDir, getCsvFormat(FILE_TYPE_CASE), "ODSCode");
        List<File> splitCaseFiles = csvSplitter.go();

        //save any new ODS codes so it's expected next time
        for (File splitCaseFile: splitCaseFiles) {
            String odsCode = splitCaseFile.getParent();
            if (!expectedOdsCodes.contains(odsCode)) {
                expectedOdsCodes.add(odsCode);
                db.saveAdastraOdsCode(dbConfiguration.getConfigurationId(), odsCode);
            }
        }

        //split the other files according to their case ref and then re-combine back according to their ODS code
        for (BatchFile batchFile: batch.getBatchFiles()) {

            String fileName = batchFile.getFilename();
            RemoteFile remoteFile = new RemoteFile(fileName, -1, null);
            AdastraFilenameParser nameParser = new AdastraFilenameParser(false, remoteFile, dbConfiguration);
            String fileType = nameParser.generateFileTypeIdentifier();
            String filePath = FilenameUtils.concat(sourcePermDir, fileName);

            //skip case and users files
            if (fileType.equals(FILE_TYPE_CASE)) {
                //we've already split the case file, but need to ensure that every expected ODS code has a case file
                for (String odsCode: expectedOdsCodes) {
                    String expectedFile = FilenameUtils.concat(splitTempDir, odsCode);
                    expectedFile = FilenameUtils.concat(expectedFile, fileName);
                    File expectedFileObj = new File(expectedFile);
                    if (!expectedFileObj.exists()) {
                        expectedFileObj.createNewFile();
                    }
                }

            } else if (fileType.equals(FILE_TYPE_USERS)) {
                //we don't split the users file, instead creating a duplicate of it for each ODS code
                for (String odsCode: expectedOdsCodes) {
                    String copyTo = FilenameUtils.concat(splitTempDir, odsCode);
                    copyTo = FilenameUtils.concat(copyTo, fileName);
                    Files.copy(new File(filePath).toPath(), new File(copyTo).toPath(), StandardCopyOption.REPLACE_EXISTING);
                }

            } else {
                //all other files should be split by case ref then re-combined by ODS code
                String tempSplitDir = FilenameUtils.concat(splitTempDir, fileType);
                csvSplitter = new CsvSplitter(filePath, new File(tempSplitDir), getCsvFormat(fileType), "CaseRef");
                List<File> splitFiles = csvSplitter.go();

                //hash the split files by their corresponding ODS codes
                Map<String, List<File>> hmToRecombineByOds = new HashMap<>();

                for (File splitFile: splitFiles) {
                    File splitFileDir = splitFile.getParentFile();
                    String caseRef = splitFileDir.getName();
                    String odsCode = hmCaseToOds.get(caseRef);
                    if (odsCode == null) {
                        throw new Exception("Failed to find ODS code for case ref [" + caseRef + "] in " + filePath);
                    }

                    List<File> l = hmToRecombineByOds.get(odsCode);
                    if (l == null) {
                        l = new ArrayList<>();
                        hmToRecombineByOds.put(odsCode, l);
                    }
                    l.add(splitFile);
                }

                //then recombine according to ODS code
                for (String odsCode: expectedOdsCodes) {
                    List<File> filesToCombine = hmToRecombineByOds.get(odsCode);

                    String expectedFile = FilenameUtils.concat(splitTempDir, odsCode);
                    expectedFile = FilenameUtils.concat(expectedFile, fileName);
                    File expectedFileObj = new File(expectedFile);

                    if (filesToCombine == null) {
                        LOG.debug("No " + fileType + " content for " + odsCode + " so creating empty file " + expectedFileObj);
                        expectedFileObj.createNewFile();

                    } else {
                        LOG.debug("Combining " + filesToCombine.size() + " " + fileType + " files for " + odsCode + " into " + expectedFileObj);
                        CsvJoiner joiner = new CsvJoiner(filesToCombine, expectedFileObj, getCsvFormat(fileType));
                        joiner.go();
                    }
                }
            }
        }


        List<BatchSplit> ret = new ArrayList<>();

        //create a batch split for each ODS code
        for (String odsCode: expectedOdsCodes) {

            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, odsCode);

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(odsCode);
            ret.add(batchSplit);

            //copy everything to storage
            String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
            storagePath = FilenameUtils.concat(storagePath, odsCode);

            String orgDir = FilenameUtils.concat(splitTempDir, odsCode);
            File[] splitFiles = new File(orgDir).listFiles();
            LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage for " + odsCode);

            for (File splitFile: splitFiles) {

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }

        return ret;
    }

    private Map<String, String> parseCaseFile(String caseFilePath) throws Exception {

        Map<String, String> ret = new HashMap<>();

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(caseFilePath);
        CSVParser parser = new CSVParser(reader, getCsvFormat(FILE_TYPE_CASE));
        try {
            Iterator<CSVRecord> iterator = parser.iterator();
            while (iterator.hasNext()) {
                CSVRecord csvRecord = iterator.next();
                String caseRef = csvRecord.get("CaseRef");
                String odsCode = csvRecord.get("ODSCode");
                ret.put(caseRef, odsCode);
            }
            return ret;

        } finally {
            parser.close();
        }
    }

    private static CSVFormat getCsvFormat(String fileTypeIdentifier) throws Exception {
        String[] cols = getCsvHeaders(fileTypeIdentifier);
        return CSV_FORMAT.withHeader(cols);
    }

    private static String[] getCsvHeaders(String fileTypeIdentifier) throws Exception {
        switch (fileTypeIdentifier) {
            case "CASE":
                return new String[]{
                        "PatientRef",
                        "PriorityName",
                        "CaseRef",
                        "CaseNo",
                        "StartDateTime",
                        "EndDateTime",
                        "LocationName",
                        "CaseTagName",
                        "ArrivedPCC",
                        "UserRef",
                        "ODSCode"
                };
            case "CASEQUESTIONS":
                return new String[]{
                        "CaseRef",
                        "QuestionSetName",
                        "Question",
                        "Answer",
                        "SortOrder"
                };
            case "CLINICALCODES":
                return new String[]{
                        "CaseRef",
                        "ConsultationRef",
                        "ClinicalCode",
                        "Term"
                };
            case "CONSULTATION":
                return new String[]{
                        "CaseRef",
                        "ConsultationRef",
                        "StartDateTime",
                        "EndDateTime",
                        "Location",
                        "ConsultationCaseType",
                        "History",
                        "Examination",
                        "Diagnosis",
                        "TreatmentPlan",
                        "PatientName",
                        "PatientForename",
                        "PatientSurname",
                        "ProviderType",
                        "GMC",
                        "UserRef"
                };
            case "ElectronicPrescriptions":
                return new String[]{
                        "CaseRef",
                        "ProviderRef",
                        "SubmissionTime",
                        "AuthorisationTime",
                        "CancellationRequestedTime",
                        "CancellationConfirmationTime",
                        "CancellationReasonCode",
                        "CancellationReasonText",
                        "PharmacyName",
                        "PrescriberNumber",
                        "PrescriberName"
                };
            case "NOTES":
                return new String[]{
                        "CaseRef",
                        "PatientRef",
                        "ReviewDateTime",
                        "NoteText",
                        "Obsolete",
                        "Active",
                        "UserRef"
                };
            case "OUTCOMES":
                return new String[]{
                        "CaseRef",
                        "OutcomeName"
                };
            case "PATIENT":
                return new String[]{
                        "CaseRef",
                        "PatientRef",
                        "Forename",
                        "Surname",
                        "DOB",
                        "NHSNumber",
                        "NHSNoTraceStatus",
                        "Language",
                        "Ethnicity",
                        "Gender",
                        "RegistrationType",
                        "HomeAddressType",
                        "HomeAddressBuilding",
                        "HomeAddressStreet",
                        "HomeAddressTown",
                        "HomeAddressLocality",
                        "HomeAddressPostcode",
                        "CurrentAddressType",
                        "CurrentAddressBuilding",
                        "CurrentAddressStreet",
                        "CurrentAddressTown",
                        "CurrentAddressLocality",
                        "CurrentAddressPostcode",
                        "MobilePhone",
                        "HomePhone",
                        "OtherPhone",
                        "LastEditByUserRef"
                };
            case "PRESCRIPTIONS":
                return new String[]{
                        "CaseRef",
                        "ConsultationRef",
                        "DrugName",
                        "Preparation",
                        "Dosage",
                        "Quantity",
                        "DMDCode",
                        "Issue"
                };
            case "PROVIDER":
                return new String[]{
                        "PatientRef",
                        "RegistrationStatus",
                        "GPNationalCode",
                        "GPName",
                        "GPPracticeNatCode",
                        "GPPracticeName",
                        "GPPracticePostcode",
                        "CaseRef"
                };
            case "USERS":
                return new String[]{
                        "UserRef",
                        "UserName",
                        "Forename",
                        "Surname",
                        "FullName",
                        "ProviderRef",
                        "ProviderName",
                        "ProviderType",
                        "ProviderGMC",
                        "ProviderNMC"
                };
            default:
                throw new Exception("Unexpected file type identifier [" + fileTypeIdentifier + "]");
        }
    }

    private static String findCaseFile(Batch batch, String sourceTempDir, DbConfiguration dbConfiguration) throws Exception {

        for (BatchFile batchFile: batch.getBatchFiles()) {

            String fileName = batchFile.getFilename();
            RemoteFile remoteFile = new RemoteFile(fileName, -1, null);
            AdastraFilenameParser nameParser = new AdastraFilenameParser(false, remoteFile, dbConfiguration);
            String fileType = nameParser.generateFileTypeIdentifier();
            if (fileType.equals(FILE_TYPE_CASE)) {
                return FilenameUtils.concat(sourceTempDir, fileName);
            }
        }

        throw new Exception("Failed to find " + FILE_TYPE_CASE + " file in batch " + batch.getBatchId());
    }


    /**
     * For Adastra, the files in a batch are by date and time stamp for an organisation, so get orgId from the first filename
     */
    /*@Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());

        //for Adastra, the orgCode is the second piece of a file in a batch, so use the first
        List<BatchFile> batchFiles = batch.getBatchFiles();
        BatchFile firstBatchFile = batchFiles.get(0);
        String [] fileParts = firstBatchFile.getFilename().split("_");
        String orgCode = fileParts [1];

        batchSplit.setOrganisationId(orgCode);

        ret.add(batchSplit);

        return ret;
    }
*/

}
