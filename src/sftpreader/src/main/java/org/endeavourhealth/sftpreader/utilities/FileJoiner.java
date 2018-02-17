package org.endeavourhealth.sftpreader.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.List;

public class FileJoiner {

    private static final Logger LOG = LoggerFactory.getLogger(FileJoiner.class);

    private List<File> srcFiles = null;
    private File dstFile = null;
    private boolean writtenContent;
    private boolean singleHeaderOnly;

    public FileJoiner(List<File> srcFiles, File dstFile, boolean singleHeaderOnly) {
        this.srcFiles = srcFiles;
        this.dstFile = dstFile;
        this.singleHeaderOnly = singleHeaderOnly;
    }

    public boolean go() throws Exception {
        FileWriter fileWriter = null;
        BufferedWriter bufferedWriter = null;
        writtenContent = false;

        try {

            for (File srcFile : srcFiles) {

                //create the printer to the destination file if required
                if (bufferedWriter == null) {
                    fileWriter = new FileWriter(dstFile);
                    bufferedWriter = new BufferedWriter(fileWriter);
                }

                LOG.trace("Joining source file: "+srcFile);

                //simply print each record from the source into the destination
                String currLine;
                FileReader fileReader = new FileReader(srcFile);
                BufferedReader br = new BufferedReader(fileReader);
                // skip line 1 (header) ?
                if (singleHeaderOnly && writtenContent) {
                    currLine = br.readLine();
                }
                while ((currLine = br.readLine()) != null) {
                    bufferedWriter.write(currLine);
                    bufferedWriter.newLine();
                    writtenContent = true;
                }
                br.close();
                fileReader.close();
            }

        } finally {

            //make sure everything is closed
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
            if (fileWriter != null) {
                fileWriter.close();
            }
        }

        return writtenContent;
    }
}
