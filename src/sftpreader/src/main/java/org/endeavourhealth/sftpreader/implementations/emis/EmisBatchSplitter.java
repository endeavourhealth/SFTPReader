package org.endeavourhealth.sftpreader.implementations.emis;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisConstants;
import org.endeavourhealth.sftpreader.implementations.emis.utility.EmisHelper;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpFilenameParseException;
import org.endeavourhealth.sftpreader.utilities.CsvJoiner;
import org.endeavourhealth.sftpreader.utilities.CsvSplitter;
import org.endeavourhealth.sftpreader.utilities.RemoteFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class EmisBatchSplitter extends SftpBatchSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(EmisBatchSplitter.class);

    private static final String SPLIT_COLUMN_ORG = "OrganisationGuid";
    private static final String SPLIT_COLUMN_PROCESSING_ID = "ProcessingId";

    public static final String SPLIT_FOLDER = "Split";

    private static List<String> agreedOrgIds = new ArrayList<>();

    /**
     * splits the EMIS extract files we use by org GUID and processing ID, so
     * we have a directory structure of dstDir -> org GUID -> processing ID
     * returns a list of directories containing split file sets
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        //the big CSV files will already be in our temp storage because we'll have just decrypted the GPG files there
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

        //scan through the files in the folder and works out which are admin and which are clinical
        List<String> processingIdFiles = new ArrayList<>();
        List<String> orgAndProcessingIdFiles = new ArrayList<>();
        List<String> orgIdFiles = new ArrayList<>();
        Map<String, String> headersCache = new HashMap<>();

        identifyFiles(batch, sourceTempDir, orgAndProcessingIdFiles, processingIdFiles, orgIdFiles, dbConfiguration, headersCache);

        Set<File> orgIdDirs = new HashSet<>();
        Map<File, Set<File>> processingIdDirsByOrgId = new HashMap<>();

        //split the org ID-only files (i.e. sharing agreements file) so we have a directory per organisation ID
        for (String fileName: orgIdFiles) {
            LOG.trace("Splitting " + fileName + " into " +  dstDir);
            List<File> splitFiles = splitFile(fileName, dstDir, EmisConstants.CSV_FORMAT, SPLIT_COLUMN_ORG);
            appendOrgIdsToSet(splitFiles, orgIdDirs);
        }

        //splitting the sharing agreements file will have created a folder for every org listed,
        //including the non-active ones in there. So delete any folder for orgs that aren't active in the sharing agreement
        Set<File> expectedOrgFolders = findExpectedOrgFolders(instanceConfiguration, dbConfiguration, batch, dstDir);
        for (File orgDir: orgIdDirs) {
            if (!expectedOrgFolders.contains(orgDir)) {
                FileHelper.deleteRecursiveIfExists(orgDir);
            }
        }
        orgIdDirs = expectedOrgFolders;
        LOG.trace("Created " + orgIdDirs.size() + " org ID folders");

        //split the clinical files by org and processing ID, which creates the org ID -> processing ID folder structure
        for (String sourceFilePath: orgAndProcessingIdFiles) {
            LOG.trace("Splitting " + sourceFilePath + " into " + dstDir);
            List<File> splitFiles = splitFile(sourceFilePath, dstDir, EmisConstants.CSV_FORMAT, SPLIT_COLUMN_ORG, SPLIT_COLUMN_PROCESSING_ID);

            String fileName = FilenameUtils.getName(sourceFilePath);

            //having split the file, we then want to join the files back together so we have one per
            //organisation but ordered by processing ID
            for (File orgDir: orgIdDirs) {
                File joinedFile = new File(orgDir, fileName);
                List<File> splitFilesForOrg = filterOrgAndProcessingFilesByParent(splitFiles, orgDir);

                joinFiles(joinedFile, splitFilesForOrg);
            }

            //keep track of all the processing ID folders we've created
            appendProcessingIdDirsToMap(splitFiles, processingIdDirsByOrgId);
        }

        //for the files with just a processing ID, each org folder we want a copy of the non-clinical data, but in processing ID order
        for (String sourceFilePath : processingIdFiles) {

            File reorderedFile = null;

            String fileName = FilenameUtils.getName(sourceFilePath);

            for (File orgDir: orgIdDirs) {

                //if we've not split and re-ordered the file, do it now into this org dir
                if (reorderedFile == null) {
                    LOG.trace("Splitting processing ID file " + sourceFilePath + " into " + orgDir);
                    List<File> splitFiles = splitFile(sourceFilePath, orgDir, EmisConstants.CSV_FORMAT, SPLIT_COLUMN_PROCESSING_ID);
                    LOG.trace("Splitting into " + splitFiles.size() + " files");

                    //join them back together
                    File joinedFile = new File(orgDir, fileName);

                    reorderedFile = joinFiles(joinedFile, splitFiles);
                    LOG.trace("Joined back into " + reorderedFile);
                    if (reorderedFile != null) {
                        LOG.trace("Joined file size " + reorderedFile.length());
                    }

                    appendProcessingIdDirsToMap(splitFiles, processingIdDirsByOrgId);

                    //if the file was empty, there won't be a reordered file, so just drop out, and let the
                    //thing that creates empty files pick this up
                    if (reorderedFile == null) {
                        break;
                    }

                } else {
                    //if we have split and re-ordered the file, just copy it into this org dir
                    File orgFile = new File(orgDir, fileName);
                    Files.copy(reorderedFile.toPath(), orgFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
                }
            }
        }

        //each org dir with have loads of empty folders for the processing IDs, so delete them
        //iterate using a copy of the orgIdsFolders set, because we'll want to remove from the set while iterating
        for (File orgDir : new HashSet<>(orgIdDirs)) {
            //LOG.trace("Deleting processing ID folders from " + orgDir);

            Set<File> processingIdDirs = processingIdDirsByOrgId.get(orgDir);
            if (processingIdDirs != null) {
                //for (File processingIdDir: orgDir.listFiles()) {
                for (File processingIdDir : processingIdDirs) {
                    //LOG.trace("Checking " + processingIdDir);
                    if (processingIdDir.listFiles().length > 0) {
                        throw new Exception("Processing ID dir " + processingIdDir + " isn't empty");
                    }
                    LOG.trace("Going to delete " + processingIdDir);
                    FileHelper.deleteRecursiveIfExists(processingIdDir);
                }
            }

            //the sharing agreements file always has a row per org in the data sharing agreement, even if there
            //isn't any data for that org in the extract. So we'll have just created a folder for that org
            //and it'll now be empty. So delete any empty org directory.
            if (orgDir.listFiles().length == 0) {
                LOG.trace("Org dir is now empty, so will delete it");
                FileHelper.deleteRecursiveIfExists(orgDir);
                orgIdDirs.remove(orgDir);
            }
        }

        //the splitter only creates files when required, so we'll have incomplete file sets,
        //so create any missing files, so there's a full set of files in every folder
        for (BatchFile batchFile: batch.getBatchFiles()) {
            String fileName = EmisFilenameParser.getDecryptedFileName(batchFile, dbConfiguration);
            String headers = headersCache.get(fileName);

            //iterate through any directories, creating any missing files in their sub-directories
            for (File orgDir: orgIdDirs) {
                createMissingFile(fileName, headers, orgDir);
            }
        }

        //if any of our ORG folders contains child folders, then something has gone wrong with the splitting and joining,
        //so check for this and throw an exception if this is the case
        validateSplitFolders(orgIdDirs);

        LOG.trace("Completed CSV file splitting to " + dstDir);

        //we need to parse the organisation file, to store the mappings for later
        saveAllOdsCodes(db, instanceConfiguration, dbConfiguration, batch);

        //build a list of the folders containing file sets, to return
        List<BatchSplit> ret = new ArrayList<>();

        for (File orgDir : orgIdDirs) {

            String orgGuid = orgDir.getName();
            String localPath = FilenameUtils.concat(batch.getLocalRelativePath(), SPLIT_FOLDER);
            localPath = FilenameUtils.concat(localPath, orgGuid);

            //we need to find the ODS code for the EMIS org GUID. When we have a full extract, we can find that mapping
            //in the Organisation CSV file, but for deltas, we use the key-value-pair table which is populated when we get the deltas
            EmisOrganisationMap org = findOrg(orgGuid, db);

            //we've received at least one set of data for a service we don't recognise
            if (org == null) {
                throw new RuntimeException("Failed to find organisation record for EMIS Org GUID " + orgGuid);
            }

            String odsCode = org.getOdsCode();

            BatchSplit batchSplit = new BatchSplit();
            batchSplit.setBatchId(batch.getBatchId());
            batchSplit.setLocalRelativePath(localPath);
            batchSplit.setOrganisationId(odsCode);

            ret.add(batchSplit);

            //copy everything to storage
            String storagePath = FilenameUtils.concat(sourcePermDir, SPLIT_FOLDER);
            storagePath = FilenameUtils.concat(storagePath, orgGuid);

            File[] splitFiles = orgDir.listFiles();
            LOG.trace("Copying " + splitFiles.length + " files from " + orgDir + " to permanent storage for " + odsCode);

            for (File splitFile: splitFiles) {

                String fileName = splitFile.getName();
                String storageFilePath = FilenameUtils.concat(storagePath, fileName);

                FileHelper.writeFileToSharedStorage(storageFilePath, splitFile);
            }
        }

        return ret;
    }





    private void appendProcessingIdDirsToMap(List<File> splitFiles, Map<File, Set<File>> processingIdDirsByOrgId) {
        for (File splitFile: splitFiles) {

            File processingIdDir = splitFile.getParentFile();
            File orgIdDir = processingIdDir.getParentFile();

            Set<File> processingIdDirs = processingIdDirsByOrgId.get(orgIdDir);
            if (processingIdDirs == null) {
                processingIdDirs = new HashSet<>();
                processingIdDirsByOrgId.put(orgIdDir, processingIdDirs);
            }
            processingIdDirs.add(processingIdDir);
        }
    }

    private List<File> filterOrgAndProcessingFilesByParent(List<File> splitFiles, File requiredOrgDir) {
        List<File> ret = new ArrayList<>();

        for (File splitFile: splitFiles) {
            File processingIdDir = splitFile.getParentFile();
            File orgDir = processingIdDir.getParentFile();
            if (orgDir.equals(requiredOrgDir)) {
                ret.add(splitFile);
            }
        }

        return ret;
    }

    private static void appendOrgIdsToSet(List<File> splitFiles, Set<File> orgIdsFolders) {
        for (File splitFile: splitFiles) {

            File orgIdFolder = splitFile.getParentFile();
            orgIdsFolders.add(orgIdFolder);
        }
    }

    /**
     * validates that each org dir contains no sub-directories, that every folder contains the same number of files
     * and that the total size of the split files is larger than the original (it'll be larger due to duplication
     * of CSV header lines and that some files aren't split by org)
     */
    private void validateSplitFolders(Set<File> orgIdsFolders) throws Exception {

        Map<String, Long> fileNamesAndSizes = new HashMap<>();

        for (File orgDir : orgIdsFolders) {

            File[] orgDirContents = orgDir.listFiles();
            if (!fileNamesAndSizes.isEmpty()
                && fileNamesAndSizes.size() != orgDirContents.length) {
                throw new Exception("Organisation dir " + orgDir + " contains " + orgDirContents.length + " but others contain " + fileNamesAndSizes.size());
            }

            for (File orgDirChild: orgDirContents) {

                if (orgDirChild.isDirectory()) {
                    throw new Exception("Organisation dir " + orgDir + " should not contain subdirectories (" + orgDirChild.getName() + ")");
                }

                String name = orgDirChild.getName();
                long size = orgDirChild.length();

                Long totalSize = fileNamesAndSizes.get(name);
                if (totalSize == null) {
                    fileNamesAndSizes.put(name, new Long(size));

                } else {
                    fileNamesAndSizes.put(name, new Long(size + totalSize.longValue()));
                }
            }
        }

        //backed out this check, since in some cases the split files do end up smaller than the original one,
        //because emis send duplicate rows (at least in the sessionUser file) which the splitter ignores
        /*for (String fileName: fileNamesAndSizes.keySet()) {
            Long totalSize = fileNamesAndSizes.get(fileName);

            File srcFile = new File(srcDir, fileName);
            long srcSize = srcFile.length();

            if (totalSize.longValue() < srcSize) {
                throw new Exception("Split " + fileName + " files add up to less than the original size");
            }
        }*/
    }

    private static File joinFiles(File joinedFile, List<File> splitFiles) throws Exception {

        List<File> separateFiles = orderFilesByProcessingId(splitFiles);

        CsvJoiner joiner = new CsvJoiner(separateFiles, joinedFile, EmisConstants.CSV_FORMAT.withHeader());
        boolean joined = joiner.go();

        //delete all the separate files
        for (File orgProcessingIdFile: separateFiles) {
            FileHelper.deleteRecursiveIfExists(orgProcessingIdFile);
        }

        if (joined) {
            return joinedFile;
        } else {
            return null;
        }
    }

    private static List<File> orderFilesByProcessingId(List<File> files) {

        //the org directory contains a sub-directory for each processing ID, which must be processed in order
        List<Integer> processingIds = new ArrayList<>();
        Map<Integer, File> hmFiles = new HashMap<>();

        for (File file: files) {
            File parent = file.getParentFile();
            String processingIdStr = parent.getName();
            Integer processingId = Integer.valueOf(processingIdStr);

            processingIds.add(processingId);
            hmFiles.put(processingId, file);
        }

        Collections.sort(processingIds);

        List<File> ret = new ArrayList<>();

        for (Integer processingId: processingIds) {
            File f = hmFiles.get(processingId);
            ret.add(f);
        }

        return ret;
    }

    /*private static List<File> findDirectoriesAndOrderByNumber(File rootDir) {

        //the org directory contains a sub-directory for each processing ID, which must be processed in order
        List<Integer> processingIds = new ArrayList<>();
        Map<Integer, File> hmFiles = new HashMap<>();

        for (File file: rootDir.listFiles()) {
            if (file.isDirectory()) {
                Integer processingId = Integer.valueOf(file.getName());
                processingIds.add(processingId);
                hmFiles.put(processingId, file);
            }
        }

        Collections.sort(processingIds);

        List<File> ret = new ArrayList<>();

        for (Integer processingId: processingIds) {
            File f = hmFiles.get(processingId);
            ret.add(f);
        }

        return ret;
    }*/


    /*public static File findSharingAgreementsFile(Batch batch, String rootPath) {

        String batchRootPath = FilenameUtils.concat(rootPath, batch.getLocalRelativePath());

        for (BatchFile batchFile: batch.getBatchFiles()) {
            if (batchFile.getFileTypeIdentifier().equalsIgnoreCase(EMIS_AGREEMENTS_FILE_ID)) {

                String path = FilenameUtils.concat(batchRootPath, batchFile.getDecryptedFilename());
                return new File(path);
            }
        }

        return null;
    }*/

    /**
     * goes through the sharing agreements file to find the org GUIDs of those orgs activated in the sharing agreement
     */
    private Set<File> findExpectedOrgFolders(DbInstanceEds instanceConfiguration,
                                             DbConfiguration dbConfiguration,
                                             Batch batch,
                                             File splitTempDir) throws Exception {

        String sharingAgreementFile = EmisHelper.findPreSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, EmisConstants.SHARING_AGREEMENTS_FILE_TYPE);

        Set<File> ret = new HashSet<>();

        InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(sharingAgreementFile);
        CSVParser csvParser = new CSVParser(isr, EmisConstants.CSV_FORMAT.withHeader());

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                String orgGuid = csvRecord.get("OrganisationGuid");
                String activated = csvRecord.get("IsActivated");
                if (activated.equalsIgnoreCase("true")) {

                    File orgDir = new File(splitTempDir, orgGuid);
                    ret.add(orgDir);
                    agreedOrgIds.add(orgGuid);
                }
            }
        } finally {
            csvParser.close();
        }

        return ret;
    }

    private static void saveAllOdsCodes(DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, Batch batch) throws Exception {

        //go through our Admin_Organisation file, saving all new org details to our PostgreSQL DB
        String adminFilePath = EmisHelper.findPreSplitFileInTempDir(instanceConfiguration, dbConfiguration, batch, EmisConstants.ADMIN_ORGANISATION_FILE_TYPE);

        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(adminFilePath);
        CSVParser csvParser = new CSVParser(reader, EmisConstants.CSV_FORMAT.withHeader());

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                String orgGuid = csvRecord.get("OrganisationGuid");
                String orgName = csvRecord.get("OrganisationName");
                String orgOds = csvRecord.get("ODSCode");
                String orgCdb = csvRecord.get("CDB");

                String combinedName = orgName;
                if (!Strings.isNullOrEmpty(orgCdb)) {
                    combinedName += " (CDB " + orgCdb + ")";
                }

                if (!StringUtils.isNotEmpty(orgOds)) {
                    continue;
                }

                //We had the ODS code for F86644 swap to F86644a and back again, resulting in data split over
                //two services, so ignore it changing to the wrong version again
                if (orgOds.equalsIgnoreCase("F86644a")) {
                    continue;
                }

                //SD-391 - got a very odd looking ODS code change for this practice (changing from a valid active GP Practice ODS code
                //to one for a closed bankrupt GP practice). Until we get clarity from Emis that the change is correct, suppress this change.
                if (orgOds.equalsIgnoreCase("E84670")) {
                    continue;
                }

                //Validate that the ODS code isn't changing. We want to keep this table updated with name
                //changes etc., but if the ODS code changes, then we need to understand what's happening and
                //manually reconfigure the protocol to expect the new ODS code (and potentially move data)
                EmisOrganisationMap existingMapping = db.getEmisOrganisationMap(orgGuid);
                if (existingMapping != null) {
                    String existingOdsCode = existingMapping.getOdsCode();
                    if (!existingOdsCode.equalsIgnoreCase(orgOds)) {
                        //if this happens, we need to work out if it's a permanent ODS code change
                        //or a weird temporary one like happened for F86644 (which changed to F86644a for a day)
                        // Only throw exception if org has a sharing agreement hence active
                        if (agreedOrgIds.contains(orgGuid)) {
                            String alert = "ODS code for " + orgName + " has changed from " + existingOdsCode + " to " + orgOds + " and needs manually handling";
                            SlackHelper.sendSlackMessage(SlackHelper.Channel.SftpReaderAlerts, alert);
                            throw new Exception(alert);
                        } else {
                            LOG.error("ODS code for " + orgName + " has changed from " + existingOdsCode + " to " + orgOds);
                        }
                     }
                }

                //create and save the mapping
                EmisOrganisationMap mapping = new EmisOrganisationMap()
                        .setGuid(orgGuid)
                        .setName(combinedName)
                        .setOdsCode(orgOds);

                db.addEmisOrganisationMap(mapping);

            }
        } finally {
            csvParser.close();
        }
    }

    public static EmisOrganisationMap findOrg(String emisOrgGuid, DataLayerI db) throws Exception {

        //look in our mapping table to find the ODS code for our org GUID
        return db.getEmisOrganisationMap(emisOrgGuid);
    }

    /**
     * scans through the files in the folder and works out which are admin and which are clinical
     */
    private static void identifyFiles(Batch batch, String sourceTempDir, List<String> orgAndProcessingIdFiles, List<String> processingIdFiles,
                                      List<String> orgIdFiles, DbConfiguration dbConfiguration, Map<String, String> headersCache) throws Exception {

        for (BatchFile batchFile: batch.getBatchFiles()) {

            String fileName = EmisFilenameParser.getDecryptedFileName(batchFile, dbConfiguration);

            //first try to use a file in our temporary storage
            String filePath = FilenameUtils.concat(sourceTempDir, fileName);

            RemoteFile remoteFile = new RemoteFile(fileName, -1, null);
            EmisFilenameParser nameParser = new EmisFilenameParser(false, remoteFile, dbConfiguration);
            String fileType = nameParser.generateFileTypeIdentifier();

            //we work out what columns to split by, by looking at the CSV file headers
            String fileStart = FileHelper.readFirstCharactersFromSharedStorage(filePath, 1024);
            StringReader reader = new StringReader(fileStart);
            //InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(filePath);
            CSVParser csvParser = new CSVParser(reader, EmisConstants.CSV_FORMAT.withHeader());
            try {
                Map<String, Integer> headers = csvParser.getHeaderMap();

                boolean splitByOrgId = headers.containsKey(SPLIT_COLUMN_ORG)
                        && !fileType.equals("Admin_Organisation") //these three files have an OrganisationGuid column, but don't want splitting by it
                        && !fileType.equals("Admin_OrganisationLocation")
                        && !fileType.equals("Admin_UserInRole");

                boolean splitByProcessingId = headers.containsKey(SPLIT_COLUMN_PROCESSING_ID);

                if (splitByOrgId && splitByProcessingId) {
                    orgAndProcessingIdFiles.add(filePath);

                } else if (splitByOrgId) {
                    orgIdFiles.add(filePath);

                } else if (splitByProcessingId) {
                    processingIdFiles.add(filePath);

                } else {
                    throw new SftpFilenameParseException("Unknown EMIS CSV file type for " + fileName);
                }

            } finally {
                csvParser.close();
            }

            //we also need to cache the headers for later, when we're creating missing files, so turn the map into a String and add to the map
            BufferedReader bufferedReader = null;
            try {
                bufferedReader = new BufferedReader(new StringReader(fileStart));
                String headersStr = bufferedReader.readLine();
                headersCache.put(fileName, headersStr);
            } finally {
                bufferedReader.close();
            }
        }
    }


    private static void createMissingFile(String fileName, String headers, File dstDir) throws Exception {

        File dstFile = new File(dstDir, fileName);
        if (dstFile.exists()) {
            return;
        }

        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;

        try {

            fileWriter = new FileWriter(dstFile);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.append(headers);
            bufferedWriter.newLine();

        } finally {

            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    private static String readFileHeaders(File srcFile) throws Exception {

        String headers = null;
        BufferedReader bufferedReader = null;
        try {
            bufferedReader = new BufferedReader(new FileReader(srcFile));
            headers = bufferedReader.readLine();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        return headers;
    }

    private static List<File> splitFile(String sourceFilePath, File dstDir, CSVFormat csvFormat, String... splitColmumns) throws Exception {
        CsvSplitter csvSplitter = new CsvSplitter(sourceFilePath, dstDir, true, csvFormat.withHeader(), splitColmumns);
        return csvSplitter.go();
    }
}
