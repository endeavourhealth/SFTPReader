package org.endeavourhealth.sftpreader.implementations.emis.utility;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.sftpreader.implementations.emis.EmisBatchSplitter;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class SharingAgreementRecord {

    private String guid;
    private boolean activated;
    private boolean disabled;
    private boolean deleted;

    public SharingAgreementRecord(String guid, boolean activated, boolean disabled, boolean deleted) {
        this.guid = guid;
        this.activated = activated;
        this.disabled = disabled;
        this.deleted = deleted;
    }

    public String getGuid() {
        return guid;
    }

    public boolean isActivated() {
        return activated;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public boolean isDeleted() {
        return deleted;
    }

    @Override
    public String toString() {
        return "Activated = " + activated + " Disabled = " + disabled + " Deleted = " + deleted;
    }


    public static Map<String, SharingAgreementRecord> readSharingAgreementsFile(String filePath) throws SftpValidationException {

        Map<String, SharingAgreementRecord> ret = new HashMap<>();

        CSVParser csvParser = null;
        try {
            InputStreamReader isr = FileHelper.readFileReaderFromSharedStorage(filePath);
            csvParser = new CSVParser(isr, EmisConstants.CSV_FORMAT.withHeader());

            Iterator<CSVRecord> csvIterator = csvParser.iterator();

            while (csvIterator.hasNext()) {
                CSVRecord csvRecord = csvIterator.next();

                String orgGuid = csvRecord.get("OrganisationGuid");
                String activated = csvRecord.get("IsActivated");
                String disabled = csvRecord.get("Disabled");
                String deleted = csvRecord.get("Deleted");

                SharingAgreementRecord r = new SharingAgreementRecord(orgGuid, activated.equalsIgnoreCase("true"), disabled.equalsIgnoreCase("true"), deleted.equalsIgnoreCase("true"));
                ret.put(orgGuid, r);
            }
        } catch (Exception ex) {
            throw new SftpValidationException("Failed to read sharing agreements file " + filePath, ex);

        } finally {
            try {
                if (csvParser != null) {
                    csvParser.close();
                }
            } catch (IOException ioe) {
                //if we fail to close, then it doesn't matter
            }
        }

        return ret;
    }
}
