package org.endeavourhealth.sftpreader.implementations.barts;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpNotificationCreator;
import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class BartsSftpNotificationCreator extends SftpNotificationCreator {
    private static final Logger LOG = LoggerFactory.getLogger(BartsSftpNotificationCreator.class);

    @Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        //we have multiple file fragments and occasionally get the same file type sent twice
        //in the same day, so we need to ensure they're properly ordered, so that the queue reader
        //can just process them in the order supplied
        List<String> files = super.findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, null);

        //the 2.1 and 2.2 files have different naming systems, but if we simply compare on any numeric element
        //of the file names, we get a consistent ordering that works for both
        List<FileWrapper> fileWrappers = new ArrayList<>();
        for (String file: files) {
            fileWrappers.add(new FileWrapper(file));
        }

        Collections.sort(fileWrappers);

        files = new ArrayList<>();
        for (FileWrapper fileWrapper: fileWrappers) {
            files.add(fileWrapper.getFileName());
        }

        return super.combineFilesForNotificationMessage(files);
        //return super.createDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, null);
    }


    static class FileWrapper implements Comparable {
        private String fileName = null;
        private List<Long> numericElements = null;

        public FileWrapper(String fileName) {
            this.fileName = fileName;

            //parse out any numeric elements
            this.numericElements = new ArrayList<>();

            String[] toks = fileName.split("[._-]"); //split by dot, underscore and dash
            for (String tok: toks) {
                try {
                    Long l = Long.valueOf(tok);
                    this.numericElements.add(l);

                } catch (NumberFormatException nfe) {
                    //skip it
                }
            }
        }



        public String getFileName() {
            return fileName;
        }

        @Override
        public int compareTo(Object o) {
            if (o instanceof FileWrapper) {
                List<Long> ourElements = this.numericElements;
                List<Long> otherElements = ((FileWrapper)o).numericElements;
                if (ourElements.size() > otherElements.size()) {
                    return 1;

                } else if (ourElements.size() < otherElements.size()) {
                    return -1;

                } else {
                    for (int i=0; i<ourElements.size(); i++) {
                        Long ourElement = ourElements.get(i);
                        Long otherElement = otherElements.get(i);
                        int elementComp = ourElement.compareTo(otherElement);
                        if (elementComp != 0) {
                            return elementComp;
                        }
                    }
                }

            }
            return 0;
        }
    }

    /*@Override
    public String createNotificationMessage(String organisationId, DataLayer db, DbInstanceEds instanceConfiguration,
                                            DbConfiguration dbConfiguration, BatchSplit batchSplit) throws Exception {

        List<String> files = super.findFilesForDefaultNotificationMessage(instanceConfiguration, dbConfiguration, batchSplit, null);

        //find all the files that we combined
        List<String> combinedFilePrefixes = new ArrayList<>();
        for (String file: files) {
            String baseName = FilenameUtils.getBaseName(file); //file name without extension
            if (baseName.endsWith(BartsSftpBatchSplitter.COMBINED)) {
                LOG.debug("Found combined file " + file);
                baseName = baseName.replace(BartsSftpBatchSplitter.COMBINED, "");
                combinedFilePrefixes.add(baseName);
            }
        }

        //for each combined file, remove the fragments that were used to create it
        for (String combinedFilePrefix: combinedFilePrefixes) {
            for (int i=files.size()-1; i>=0; i--) {
                String file = files.get(i);
                String baseName = FilenameUtils.getBaseName(file); //file name without extension
                if (baseName.startsWith(combinedFilePrefix)
                        && !baseName.endsWith(BartsSftpBatchSplitter.COMBINED)) {
                    LOG.debug("Removing " + file + " as it was combined");
                    files.remove(i);
                }
            }
        }

        return super.combineFilesForNotificationMessage(files);
    }*/

}
