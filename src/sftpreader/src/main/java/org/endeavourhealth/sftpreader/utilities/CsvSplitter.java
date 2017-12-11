package org.endeavourhealth.sftpreader.utilities;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.common.utility.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

public class CsvSplitter {

    private static final Logger LOG = LoggerFactory.getLogger(CsvSplitter.class);

    private String srcFilePath = null;
    private File dstDir = null;
    private CSVFormat csvFormat = null;
    private String[] splitColumns = null;
    private String[] columnHeaders = null;
    private Map<String, CSVPrinter> csvPrinterMap = new HashMap<>();
    private Set<File> filesCreated = null;


    public CsvSplitter(String srcFilePath, File dstDir, CSVFormat csvFormat, String... splitColumns) {

        this.srcFilePath = srcFilePath;
        this.dstDir = dstDir;
        this.csvFormat = csvFormat;
        this.splitColumns = splitColumns;
    }

    public Set<File> go() throws Exception {

        //adding .withHeader() to the csvFormat forces it to treat the first row as the column headers,
        //and read them in, instead of ignoring them
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(srcFilePath);
        CSVParser csvParser = new CSVParser(reader, csvFormat.withHeader());
        filesCreated = new HashSet<>();

        try
        {
            //validate the split columns are present
            Map<String, Integer> headerMap = csvParser.getHeaderMap();
            int[] splitIndexes = new int[splitColumns.length];

            for (int i=0; i<splitColumns.length; i++) {
                String splitColumn = splitColumns[i];

                Integer columnIndex = headerMap.get(splitColumn);
                if (columnIndex == null) {
                    throw new IllegalArgumentException("No column [" + splitColumn + "] in " + srcFilePath);
                }
                splitIndexes[i] = columnIndex.intValue();
            }

            //convert the map into an ordered String array, so we can populate the column headers on new CSV files
            columnHeaders = new String[headerMap.size()];
            Iterator<String> headerIterator = headerMap.keySet().iterator();
            while (headerIterator.hasNext()) {
                String headerName = headerIterator.next();
                int headerIndex = headerMap.get(headerName);
                columnHeaders[headerIndex] = headerName;
            }

            //go through the content of the source file
            //changing to also drop duplicated lines. The EMIS test pack has huge duplication of lines (see Admin_Orgamisation)
            //and when the exact same resource is inserted into Resource_History etc. in rapid succession, we get a failure
            //so just drop any lines that are exactly the same as the previous one
            Iterator<CSVRecord> csvIterator = csvParser.iterator();
            CSVRecord previousLine = null;
            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                if (!isSame(csvRecord, previousLine)) {
                    splitRecord(csvRecord, splitIndexes);
                    previousLine = csvRecord;
                }
            }
            /*Iterator<CSVRecord> csvIterator = csvParser.iterator();
            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                splitRecord(csvRecord, splitIndexes);
            }*/


        } finally {

            //adding try/catch to get around AWS problem
            try {
                csvParser.close();
            } catch (Exception ex) {
                LOG.error("Failed to close parser " + ex.getMessage());
                try {
                    csvParser.close();
                    LOG.error("Worked on second attempt");
                } catch (Exception ex2) {
                    LOG.error("Failed on second attempt");
                    throw ex;
                }
            }

            //close all the csv printers created
            Iterator<CSVPrinter> printerIterator = csvPrinterMap.values().iterator();
            while (printerIterator.hasNext()) {
                CSVPrinter csvPrinter = printerIterator.next();

                //adding try/catch to get around AWS problem
                try {
                    csvPrinter.close();
                } catch (Exception ex) {
                    LOG.error("Failed to close printer " + ex.getMessage());
                    try {
                        csvPrinter.close();
                        LOG.error("Worked on second attempt");
                    } catch (Exception ex2) {
                        LOG.error("Failed on second attempt");
                        throw ex;
                    }
                }
            }
        }

        return filesCreated;
    }

    private static boolean isSame(CSVRecord one, CSVRecord two) {
        if (one == null
                || two == null
                || one.size() != two.size()) {
            return false;
        }

        for (int i=0; i<one.size(); i++) {
            if (!one.get(i).equals(two.get(i))) {
                return false;
            }
        }

        return true;
    }

    private void splitRecord(CSVRecord csvRecord, int[] columnIndexes) throws Exception {

        String[] values = new String[columnIndexes.length];
        for (int i=0; i<values.length; i++) {
            values[i] = csvRecord.get(columnIndexes[i]);
        }

        CSVPrinter csvPrinter = findCsvPrinter(values);
        csvPrinter.printRecord(csvRecord);
    }

    private CSVPrinter findCsvPrinter(String[] values) throws Exception {

        String mapKey = String.join("_", values);

        CSVPrinter csvPrinter = csvPrinterMap.get(mapKey);
        if (csvPrinter == null) {

            File folder = new File(dstDir.getAbsolutePath());
            for (String value: values) {

                //ensure it can be used as a valid folder name
                value = value.replaceAll("[:\\\\/*\"?|<>']", " ");
                folder = new File(folder, value);

                if (!folder.exists()
                        && !folder.mkdirs()) {
                    throw new FileNotFoundException("Couldn't create folder " + folder);
                }
            }

            String fileName = FilenameUtils.getName(srcFilePath);
            File f = new File(folder, fileName);
            filesCreated.add(f);

            //LOG.debug("Creating " + f);
            try {
                FileWriter fileWriter = new FileWriter(f);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withHeader(columnHeaders));

                csvPrinterMap.put(mapKey, csvPrinter);

            } catch (Exception ex) {
                LOG.error("Error opening writer to " + f);
                LOG.error("Currently got " + csvPrinterMap.size() + " files open");
                LOG.error("Folder exists " + folder.exists());
                LOG.error("File exists " + f.exists());

                try {
                    FileWriter fileWriter = new FileWriter(f);
                    LOG.error("Successfully opened on second attempt");
                    BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                    csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withHeader(columnHeaders));

                    csvPrinterMap.put(mapKey, csvPrinter);
                    return csvPrinter;

                } catch (Exception ex2) {
                    LOG.error("Failed to open on second attempt");
                    throw ex;
                }
            }
        }
        return csvPrinter;
    }


}
