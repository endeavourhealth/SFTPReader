package org.endeavourhealth.sftpreader.implementations.adastra.utility;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.adastra.AdastraFilenameParser;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.implementations.tpp.TppFilenameParser;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

public class AdastraHelper {
    private static final Logger LOG = LoggerFactory.getLogger(AdastraHelper.class);

    public static String findPreSplitFileInPermDir(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   Batch batch,
                                                   String fileIdentifier) throws Exception {

        String tempStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/endeavour
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/ADASTRA
        String batchPath = batch.getLocalRelativePath(); //e.g. 2021-01-21T00.00.02

        String dirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchPath);

        List<String> files = FileHelper.listFilesInSharedStorage(dirPath);

        if (files == null || files.isEmpty()) {
            throw new SftpValidationException("Failed to find any files in " + dirPath);
        }

        for (String filePath: files) {
            String name = FilenameUtils.getName(filePath);
            RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Adastra filename parsing
            AdastraFilenameParser parser = new AdastraFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return filePath;
            }
        }

        return null;
    }

    /*public static String findPreSplitFileInTempDir(DbInstanceEds instanceConfiguration,
                                                   DbConfiguration dbConfiguration,
                                                   Batch batch,
                                                   String fileIdentifier) throws SftpValidationException {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. /sftpreader/temp
        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/ADASTRA
        String batchPath = batch.getLocalRelativePath(); //e.g. 2021-01-21T00.00.02

        String tempDirPath = FileHelper.concatFilePath(tempStoragePath, configurationPath, batchPath);
        File tempDir = new File(tempDirPath);
        if (!tempDir.exists()) {
            throw new SftpValidationException("Temp directory " + tempDir + " doesn't exist");
        }

        for (File f: tempDir.listFiles()) {
            String name = f.getName();
            RemoteFile r = new RemoteFile(name, -1, null); //size and modification date aren't needed for Adastra filename parsing
            AdastraFilenameParser parser = new AdastraFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return f.getAbsolutePath();
            }
        }

        return null;
    }*/

    /**
     * finds the file of the given "type" in the temp directory structure, under the "split" directory
     */
    public static String findPostSplitFileInTempDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String tempStoragePath = instanceConfiguration.getTempDirectory(); //e.g. /sftpReader/tmp
        return findPostSplitFileInDir(tempStoragePath, dbConfiguration, batchSplit, fileIdentifier);
    }

    public static String findPostSplitFileInPermDir(DbInstanceEds instanceConfiguration,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String permStoragePath = instanceConfiguration.getSharedStoragePath(); //e.g. s3://<bucket>/root
        return findPostSplitFileInDir(permStoragePath, dbConfiguration, batchSplit, fileIdentifier);
    }

    private static String findPostSplitFileInDir(String topLevelDir,
                                                    DbConfiguration dbConfiguration,
                                                    BatchSplit batchSplit,
                                                    String fileIdentifier) throws Exception {

        String configurationPath = dbConfiguration.getLocalRootPath(); //e.g. sftpReader/Adastra/YDDH3_08W
        String batchSplitPath = batchSplit.getLocalRelativePath(); //e.g. 2019-12-09T00.15.00/Split/E87065/

        String dirPath = FileHelper.concatFilePath(topLevelDir, configurationPath, batchSplitPath);

        List<String> filePaths = FileHelper.listFilesInSharedStorage(dirPath);

        for (String filePath: filePaths) {

            /*String ext = FilenameUtils.getExtension(filePath);
            if (ext.equalsIgnoreCase("csv")) {*/

            RemoteFile r = new RemoteFile(filePath, -1, null); //size and modification date aren't needed for Adastra filename parsing
            AdastraFilenameParser parser = new AdastraFilenameParser(false, r, dbConfiguration);
            String fileType = parser.generateFileTypeIdentifier();
            if (fileType.equals(fileIdentifier)) {
                return filePath;
            }
        }

        return null;
    }


    public static CSVFormat getCsvFormat(String fileTypeIdentifier) throws Exception {
        String[] cols = getCsvHeaders(fileTypeIdentifier);
        return AdastraConstants.CSV_FORMAT.withHeader(cols);
    }

    private static String[] getCsvHeaders(String fileTypeIdentifier) throws Exception {
        switch (fileTypeIdentifier) {
            case AdastraConstants.FILE_ID_CASE:
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
            case AdastraConstants.FILE_ID_CASE_QUESTIONS:
                return new String[]{
                        "CaseRef",
                        "QuestionSetName",
                        "Question",
                        "Answer",
                        "SortOrder"
                };
            case AdastraConstants.FILE_ID_CLINICAL_CODES:
                return new String[]{
                        "CaseRef",
                        "ConsultationRef",
                        "ClinicalCode",
                        "Term"
                };
            case AdastraConstants.FILE_ID_CONSULTATION:
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
            case AdastraConstants.FILE_ID_ELECTRONIC_PRESCRIPTIONS:
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
            case AdastraConstants.FILE_ID_NOTES:
                return new String[]{
                        "CaseRef",
                        "PatientRef",
                        "ReviewDateTime",
                        "NoteText",
                        "Obsolete",
                        "Active",
                        "UserRef"
                };
            case AdastraConstants.FILE_ID_OUTCOMES:
                return new String[]{
                        "CaseRef",
                        "OutcomeName"
                };
            case AdastraConstants.FILE_ID_PATIENT:
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
            case AdastraConstants.FILE_ID_PRESCRIPTIONS:
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
            case AdastraConstants.FILE_ID_PROVIDER:
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
            case AdastraConstants.FILE_ID_USERS:
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
}
