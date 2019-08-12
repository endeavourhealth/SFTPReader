package org.endeavourhealth.sftpreader.utilities;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class CsvJoiner {

    private static final Logger LOG = LoggerFactory.getLogger(CsvJoiner.class);

    private List<File> srcFiles = null;
    private File dstFile = null;
    private CSVFormat csvFormat = null;
    private Charset encoding;

    public CsvJoiner(List<File> srcFiles, File dstFile, CSVFormat csvFormat) {
        this(srcFiles, dstFile, csvFormat, Charset.defaultCharset());
    }

    public CsvJoiner(List<File> srcFiles, File dstFile, CSVFormat csvFormat, Charset encoding) {
        this.srcFiles = srcFiles;
        this.dstFile = dstFile;
        this.csvFormat = csvFormat;
        this.encoding = encoding;

    }

    public boolean go() throws Exception {

        //the CSV format either needs to specify the headers (i.e. withHeader("col", "col")) or needs
        //to instruct that the headers are in the file itself (i.e. withHeader()).
        if (csvFormat.getHeader() == null) {
            throw new Exception("Null header in CSV format " + csvFormat);
        }

        CSVPrinter csvPrinter = null;
        CSVParser csvParser = null;
        List<String> firstColumnHeaders = null;
        boolean writtenContent = false;

        try {

            for (File srcFile : srcFiles) {

                InputStreamReader reader = FileHelper.readFileReaderFromSharedStorage(srcFile.getAbsolutePath(), encoding);
                csvParser = new CSVParser(reader, csvFormat);

                //read the headers
                Map<String, Integer> headerMap = csvParser.getHeaderMap();
                String[] columnHeadersArray = new String[headerMap.size()];
                Iterator<String> headerIterator = headerMap.keySet().iterator();
                while (headerIterator.hasNext()) {
                    String headerName = headerIterator.next();
                    int headerIndex = headerMap.get(headerName);
                    columnHeadersArray[headerIndex] = headerName;
                }

                List<String> columnHeadersList = Arrays.asList(columnHeadersArray);

                //create the printer to the destination file if required
                if (csvPrinter == null) {
                    FileOutputStream fos = new FileOutputStream(dstFile);
                    OutputStreamWriter osw = new OutputStreamWriter(fos, encoding);
                    BufferedWriter bufferedWriter = new BufferedWriter(osw);

                    if (csvFormat.getHeader().length == 0) {
                        //if the CSV format has a zero-length header, then it tells us the header is in the source files
                        //and we should explicitly carry this over to the target file
                        csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withHeader(columnHeadersArray));

                    } else {
                        //if the CSV format defines the headers itself, then it tells us the source files don't
                        //have headers and we shouldn't write headers to the target file either
                        csvPrinter = new CSVPrinter(bufferedWriter, csvFormat.withSkipHeaderRecord());
                    }

                    firstColumnHeaders = columnHeadersList;

                } else {
                    //validate that the column headers are the same as for the first file
                    if (!columnHeadersList.equals(firstColumnHeaders)) {
                        throw new Exception("Column headers for " + srcFile + " aren't the same as the first file");
                    }
                }

                //LOG.trace("Joining source file: "+srcFile);

                //simply print each record from the source into the destination
                Iterator<CSVRecord> csvIterator = csvParser.iterator();
                while (csvIterator.hasNext()) {
                    CSVRecord csvRecord = csvIterator.next();
                    csvPrinter.printRecord(csvRecord);
                    writtenContent = true;
                }

                csvParser.close();
            }

        } finally {

            //make sure everything is closed
            if (csvParser != null) {
                csvParser.close();
            }
            if (csvPrinter != null) {
                csvPrinter.close();
            }
        }

        return writtenContent;
    }
}
