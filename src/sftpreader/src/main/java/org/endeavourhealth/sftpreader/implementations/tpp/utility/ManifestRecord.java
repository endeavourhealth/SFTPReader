package org.endeavourhealth.sftpreader.implementations.tpp.utility;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ManifestRecord {

    private String fileNameWithoutExtension; //called fileName in the file, but making the name clearer
    private boolean isDelta;

    private ManifestRecord() {}

    public String getFileNameWithoutExtension() {
        return fileNameWithoutExtension;
    }

    public void setFileNameWithoutExtension(String fileNameWithoutExtension) {
        this.fileNameWithoutExtension = fileNameWithoutExtension;
    }

    public boolean isDelta() {
        return isDelta;
    }

    public void setDelta(boolean delta) {
        isDelta = delta;
    }

    public String getFileNameWithExtension() {
        return this.fileNameWithoutExtension + ".csv";
    }

    public static List<ManifestRecord> readManifestFile(File f) throws Exception {
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        InputStreamReader reader = new InputStreamReader(bis, Charset.forName(TppConstants.REQUIRED_CHARSET));
        return readManifestReader(reader);
    }

    public static List<ManifestRecord> readManifestReader(InputStreamReader reader) throws Exception {

        CSVParser csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

        List<ManifestRecord> ret = new ArrayList<>();

        try {
            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();
                String fileName = csvRecord.get("FileName");
                String isDeltaStr = csvRecord.get("IsDelta");

                ManifestRecord r = new ManifestRecord();
                r.setFileNameWithoutExtension(fileName);

                if (isDeltaStr.equalsIgnoreCase("Y")) {
                    r.setDelta(true);

                } else if (isDeltaStr.equalsIgnoreCase("N")) {
                    r.setDelta(false);

                } else {
                    //something wrong
                    throw new Exception("Unexpected value [" + isDeltaStr + "] in manifest file");
                }

                ret.add(r);
            }
        } finally {
            csvParser.close();
        }

        return ret;
    }
}
