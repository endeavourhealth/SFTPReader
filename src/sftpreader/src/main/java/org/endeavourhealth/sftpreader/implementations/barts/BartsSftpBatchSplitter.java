package org.endeavourhealth.sftpreader.implementations.barts;

import com.google.common.collect.Lists;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.utilities.CsvJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;

public class BartsSftpBatchSplitter extends SftpBatchSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpBatchSplitter.class);

    public static final String COMBINED = "COMBINED";

    private static CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                            .withDelimiter('|')
                                            .withEscape((Character)null)
                                            .withQuote((Character)null)
                                            .withQuoteMode(QuoteMode.NONE);

    /**
     * Confusingly enough, we actually COMBINE separate barts files
     */
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayer db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        LOG.debug("Going to split batch " + batch.getBatchId() + " " + batch.getBatchIdentifier());
        Map<String, List<String>> hmFilesToCombine = findFilesToCombine(batch.getBatchFiles());
        LOG.debug("Found " + hmFilesToCombine + " potential files to combine");

        for (String combinedName: hmFilesToCombine.keySet()) {
            List<String> filesToCombine = hmFilesToCombine.get(combinedName);

            //only combine if there's more than one file
            if (filesToCombine.size() > 1) {
                combineFiles(combinedName, filesToCombine, batch, instanceConfiguration, dbConfiguration);
            }
        }

        //create a single batch split and return it
        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());
        batchSplit.setOrganisationId("R1H");

        ret.add(batchSplit);

        return ret;
    }

    private Map<String, List<String>> findFilesToCombine(List<BatchFile> batchFiles) throws Exception {
        Map<String, List<String>> hmFilesToCombine = new HashMap<>();

        //loop through all the files, looking first for all the files ending _1, then _2 etc.
        int indexToFind = 1;
        while (true) {

            boolean foundSomething = false;

            for (BatchFile batchFile: batchFiles) {
                String fileName = batchFile.getFilename();
                String baseName = FilenameUtils.getBaseName(fileName); //name with no extension
                String extension = FilenameUtils.getExtension(fileName);

                List<String> toks = Lists.newArrayList(baseName.split("_"));
                String lastElement = toks.remove(toks.size()-1);
                if (lastElement.equals("" + indexToFind)) {

                    foundSomething = true;

                    //generate the name we'll use for the combined file
                    toks.add(COMBINED);
                    String combinedName = String.join("_", toks) + "." + extension;

                    List<String> list = hmFilesToCombine.get(combinedName);
                    if (list == null) {
                        list = new ArrayList<>();
                        hmFilesToCombine.put(combinedName, list);
                    }

                    //make sure we aren't missing a file somehow
                    if (list.size()+1 < indexToFind) {
                        throw new Exception("Failed to find preceeding file before " + fileName);
                    }

                    list.add(fileName);
                }
            }

            indexToFind ++;

            if (!foundSomething) {
                break;
            }
        }

        return hmFilesToCombine;
    }

    private void combineFiles(String combinedName, List<String> filesToCombine, Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        LOG.debug("Combining " + filesToCombine.size() + " -> " + combinedName);
        String sharedStorageDir = instanceConfiguration.getSharedStoragePath();
        String tempDir = instanceConfiguration.getTempDirectory();
        String configurationDir = dbConfiguration.getLocalRootPath();
        String batchDir = batch.getLocalRelativePath();

        String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        sourceTempDir = FilenameUtils.concat(sourceTempDir, batchDir);

        String sourcePermDir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        sourcePermDir = FilenameUtils.concat(sourcePermDir, batchDir);

        //check if the combined file has already been processed
//NEED TO REMOVE FOR AWS LIVE
        File permDestCombinedFile = new File(sourcePermDir, combinedName);
        boolean permDestCombinedFileExists = permDestCombinedFile.exists();
        if (permDestCombinedFileExists) {
            LOG.debug("Combined file " + combinedName + " already exists in permanent storage...skipping");
            return;
        }

        List<File> sourceFiles = new ArrayList<>();
        List<File> permSourceFiles = new ArrayList<>();
        for (String fileToCombine: filesToCombine) {

            //the raw files will need to be copied from S3 to our temp directory
            String permanentSourceFile = FilenameUtils.concat(sourcePermDir, fileToCombine);
            File permSourceFile = new File (permanentSourceFile);
            permSourceFiles.add(permSourceFile);
            File tempSourceFile = new File(sourceTempDir, fileToCombine);

            InputStream inputStream = FileHelper.readFileFromSharedStorage(permanentSourceFile);
            try {
                Files.copy(inputStream, tempSourceFile.toPath());
            } finally {
                inputStream.close();
            }

            sourceFiles.add(tempSourceFile);
        }

        File destinationFile = new File(sourceTempDir, combinedName);

        CsvJoiner joiner = new CsvJoiner(sourceFiles, destinationFile, CSV_FORMAT);
        joiner.go();

        //copy joined file to S3 if using it
        if (!FilenameUtils.equals(tempDir, sharedStorageDir)) {
            LOG.trace("Copying " + combinedName + " to permanent storage");
            String storageFilePath = FilenameUtils.concat(sourcePermDir, combinedName);
            FileHelper.writeFileToSharedStorage(storageFilePath, destinationFile);

            //delete combined file from temp
            destinationFile.delete();
            //delete source files from temp
            for (File sourceFile: sourceFiles) {
                sourceFile.delete();
            }
            //delete source files from permanent store as we have successfully combined and saved so we no longer need the parts
            for (File permSourceFile: permSourceFiles) {
                if (permSourceFile.exists()) {
                    permSourceFile.delete();
                }
            }
        }
    }

}
