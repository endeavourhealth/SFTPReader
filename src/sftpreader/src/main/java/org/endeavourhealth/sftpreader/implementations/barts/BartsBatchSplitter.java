package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchSplitter;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class BartsBatchSplitter extends SftpBatchSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(BartsBatchSplitter.class);

    @Override
    public List<BatchSplit> splitBatch(Batch batch, Batch lastCompleteBatch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //create a single batch split and return it
        List<BatchSplit> ret = new ArrayList<>();

        BatchSplit batchSplit = new BatchSplit();
        batchSplit.setBatchId(batch.getBatchId());
        batchSplit.setLocalRelativePath(batch.getLocalRelativePath());
        batchSplit.setOrganisationId("R1H");

        ret.add(batchSplit);

        return ret;
    }

    /*public static final String COMBINED = "COMBINED";

    private static CSVFormat CSV_FORMAT = CSVFormat.DEFAULT
                                .withDelimiter('|')
                                //we strictly DO NOT want any escaping, but the library requires a non-null value when using no-quotes mode so use a value that won't be in the file
                                .withEscape(Character.MAX_VALUE)
                                //.withEscape((Character)null)
                                .withQuote((Character)null)
                                .withQuoteMode(QuoteMode.NONE);



    *//**
     * Confusingly enough, we actually COMBINE separate barts files
     *//*
    @Override
    public List<BatchSplit> splitBatch(Batch batch, DataLayer db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        LOG.debug("Going to split batch " + batch.getBatchId() + " " + batch.getBatchIdentifier());
        Map<String, List<String>> hmFilesToCombine = findFilesToCombine(batch.getBatchFiles());
        LOG.debug("Found " + hmFilesToCombine.size() + " potential files to combine");

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

        if (!new File(sourceTempDir).exists()) {
            new File(sourceTempDir).mkdirs();
        }

        String sourcePermDir = FilenameUtils.concat(sharedStorageDir, configurationDir);
        sourcePermDir = FilenameUtils.concat(sourcePermDir, batchDir);

        List<File> sourceFiles = new ArrayList<>();
        for (String fileToCombine: filesToCombine) {

            //the raw files will need to be copied from S3 to our temp directory
            String permanentSourceFile = FilenameUtils.concat(sourcePermDir, fileToCombine);
            File tempSourceFile = new File(sourceTempDir, fileToCombine);
            if (tempSourceFile.exists()) {
                tempSourceFile.delete();
            }

            InputStream inputStream = FileHelper.readFileFromSharedStorage(permanentSourceFile);
            try {
                Files.copy(inputStream, tempSourceFile.toPath());
            } finally {
                inputStream.close();
            }

            sourceFiles.add(tempSourceFile);
        }

        File destinationFile = new File(sourceTempDir, combinedName);
        PrintWriter fw = new PrintWriter(destinationFile);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter pw = new PrintWriter(bw);

        boolean doneHeadings = false;

        for (File sourceFile: sourceFiles) {
            LOG.info("Reading " + sourceFile);

            FileReader fr = new FileReader(sourceFile);
            BufferedReader br = new BufferedReader(fr);
            int lineIndex = -1;

            while (true) {

                String line = br.readLine();
                lineIndex ++;
                if (line == null) {
                    break;
                }

                //we only want to carry over the first row (column headings) from the first file
                if (lineIndex == 0) {
                    if (!doneHeadings) {
                        pw.println(line);
                        doneHeadings = true;
                    }
                    continue;

                }

                pw.println(line);
            }

            br.close();
        }

        pw.close();

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
        }
    }*/

}
