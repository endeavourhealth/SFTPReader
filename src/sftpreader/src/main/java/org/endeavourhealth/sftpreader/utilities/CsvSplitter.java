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

    private static final int MAX_PRINTERS = 2000; //hard limit of printers to allow open, IO errors will prevent more than about 4000

    private String srcFilePath = null;
    private File dstDir = null;
    private CSVFormat csvFormat = null;
    private boolean tryRemoveDuplicateRecords = false;
    private String[] splitColumns = null;
    private String[] columnHeaders = null;
    private Map<String, PrinterWrapper> csvPrinterMap = new HashMap<>();
    private List<PrinterWrapper> openPrinters = new ArrayList<>();
    private List<File> filesCreated = null;
    private Charset encoding = null;
    private List<CSVRecord> lastFewRecords = new ArrayList<>();

    public CsvSplitter(String srcFilePath, File dstDir, boolean tryRemoveDuplicateRecords, CSVFormat csvFormat, String... splitColumns) {
        this(srcFilePath, dstDir, tryRemoveDuplicateRecords, csvFormat, Charset.defaultCharset(), splitColumns);
    }

    public CsvSplitter(String srcFilePath, File dstDir, boolean tryRemoveDuplicateRecords, CSVFormat csvFormat, Charset encoding, String... splitColumns) {

        this.srcFilePath = srcFilePath;
        this.dstDir = dstDir;
        this.tryRemoveDuplicateRecords = tryRemoveDuplicateRecords;
        this.csvFormat = csvFormat;
        this.splitColumns = splitColumns;
        this.encoding = encoding;
    }

    public List<File> go() throws Exception {

        //the CSV format either needs to specify the headers (i.e. withHeader("col", "col")) or needs
        //to instruct that the headers are in the file itself (i.e. withHeader()).
        if (csvFormat.getHeader() == null) {
            throw new Exception("Null header in CSV format " + csvFormat);
        }

        //adding .withHeader() to the csvFormat forces it to treat the first row as the column headers,
        //and read them in, instead of ignoring them
        InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(srcFilePath, encoding);

        CSVParser csvParser = new CSVParser(reader, csvFormat);
        filesCreated = new ArrayList<>();

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
            columnHeaders = convertCsvHeaderMapToArray(headerMap);

            //go through the content of the source file
            //changing to also drop duplicated lines. The EMIS test pack has huge duplication of lines (see Admin_Orgamisation)
            //and when the exact same resource is inserted into Resource_History etc. in rapid succession, we get a failure
            //so just drop any lines that are exactly the same as the previous one
            Iterator<CSVRecord> csvIterator = csvParser.iterator();
            CSVRecord previousLine = null;
            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                if (tryRemoveDuplicateRecords
                        && isSame(csvRecord, previousLine)) {
                    continue;
                }

                splitRecord(csvRecord, splitIndexes);
                previousLine = csvRecord;
            }

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
            for (PrinterWrapper printerWrapper: csvPrinterMap.values()) {
                printerWrapper.close();
            }
        }

        return filesCreated;
    }

    public static String[] convertCsvHeaderMapToArray(Map<String, Integer> headerMap) {

        String[] ret = new String[headerMap.size()];
        Iterator<String> headerIterator = headerMap.keySet().iterator();
        while (headerIterator.hasNext()) {
            String headerName = headerIterator.next();
            int headerIndex = headerMap.get(headerName);
            ret[headerIndex] = headerName;
        }

        return ret;
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

        //just to track errors, used above if we get an exception
        lastFewRecords.add(csvRecord);
        if (lastFewRecords.size() > 10) {
            lastFewRecords.remove(0);
        }

        String[] values = new String[columnIndexes.length];
        for (int i=0; i<values.length; i++) {
            try {
                values[i] = csvRecord.get(columnIndexes[i]);
            } catch (Exception ex) {
                for (CSVRecord previous: lastFewRecords) {
                    LOG.debug("" + previous.toString());
                }
                throw new Exception("Exception getting value " + columnIndexes[i] + " from record " + csvRecord.getRecordNumber() + " at pos " + csvRecord.getCharacterPosition(), ex);
                //LOG.error("Exception getting value " + columnIndexes[i] + " from record " + csvRecord.getRecordNumber() + " at pos " + csvRecord.getCharacterPosition());
                //return;
            }
        }


        CSVPrinter csvPrinter = findCsvPrinter(values);
        csvPrinter.printRecord(csvRecord);
    }

    private CSVPrinter findCsvPrinter(String[] values) throws Exception {

        String mapKey = String.join("_", values);

        PrinterWrapper printerWrapper = csvPrinterMap.get(mapKey);
        if (printerWrapper == null) {

            File folder = new File(dstDir.getAbsolutePath());
            for (String value : values) {

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

            printerWrapper = new PrinterWrapper(mapKey, f);
            csvPrinterMap.put(mapKey, printerWrapper);
        }

        //ensure the printer wrapper we're using is open
        if (!printerWrapper.isOpen()) {
            printerWrapper.open();
            openPrinters.add(printerWrapper);
            //LOG.debug("Opened printer for " + mapKey + ", now got " + openPrinters.size());
        }

        //on very large extracts (e.g. Emis Left & Dead) we end up with thousands of printers,
        //which hits the OS file handle limit, so we need to cap our printers
        while (openPrinters.size() >= MAX_PRINTERS) {
            PrinterWrapper oldest = openPrinters.remove(0);
            oldest.close();
            //LOG.debug("Closed printer, leaving " + openPrinters.size());
        }

        return printerWrapper.getCsvPrinter();
    }

    class PrinterWrapper {
        private String cacheKey;
        private File file;
        private CSVPrinter csvPrinter;
        private boolean needToWriteHeaders;

        public PrinterWrapper(String cacheKey, File file) {
            this.cacheKey = cacheKey;
            this.file = file;

            //if the CSV format has the headers specified as withHeader() then it means the file contains
            //the headers, in which case we also want to make sure we write out the headers in the split files
            this.needToWriteHeaders = csvFormat.getHeader().length == 0;
        }

        public CSVPrinter getCsvPrinter() {
            return csvPrinter;
        }


        public boolean isOpen() {
            return csvPrinter != null;
        }

        private void open() throws Exception {
            if (csvPrinter != null) {
                throw new IllegalArgumentException("CSVPrinter is already open");
            }

            try {
                openThrowExceptions();

            } catch (Exception ex) {
                //problems with S3FS mean this can fail but then work if we try again
                File folder = file.getParentFile();
                LOG.error("Error opening writer to " + file);
                LOG.error("Currently got " + openPrinters.size() + " printers open");
                LOG.error("Folder exists " + folder.exists());
                LOG.error("File exists " + file.exists());

                try {
                    openThrowExceptions();

                } catch (Exception ex2) {
                    LOG.error("Failed to open on second attempt");
                    throw ex;
                }
            }
        }

        private void openThrowExceptions() throws Exception {

            //only write the headers out if we've decided we need to
            if (needToWriteHeaders) {
                FileOutputStream fos = new FileOutputStream(file);
                OutputStreamWriter osw = new OutputStreamWriter(fos, encoding);
                BufferedWriter bufferedWriter = new BufferedWriter(osw);

                csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withHeader(columnHeaders));
                csvPrinter.flush(); //flush headers to disk

                //set to false so we don't write the headers out again if we re-open the file
                needToWriteHeaders = false;

            } else {
                FileOutputStream fos = new FileOutputStream(file, true);
                OutputStreamWriter osw = new OutputStreamWriter(fos, encoding);
                BufferedWriter bufferedWriter = new BufferedWriter(osw);

                csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withSkipHeaderRecord());
            }
        }

        public void close() throws Exception {
            if (csvPrinter == null) {
                return;
            }

            csvPrinter.flush();

            //try/catch to get around AWS S3FS problem
            try {
                closeThrowException();

            } catch (Exception ex) {
                LOG.error("Failed to close printer " + ex.getMessage());
                try {
                    closeThrowException();
                    LOG.error("Worked on second attempt");
                } catch (Exception ex2) {
                    LOG.error("Failed on second attempt");
                    throw ex;
                }
            }
        }

        private void closeThrowException() throws Exception {
            csvPrinter.close();
            csvPrinter = null;
        }

    }
}
