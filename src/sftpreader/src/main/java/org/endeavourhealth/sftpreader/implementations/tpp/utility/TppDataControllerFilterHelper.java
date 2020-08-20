package org.endeavourhealth.sftpreader.implementations.tpp.utility;

import com.google.common.base.Strings;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.TppOrganisationGmsRegistrationMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

/**
 * filters data out of TPP files that isn't controlled by the publisher (i.e. extract from GP practice will
 * include data from community services that the GP isn't the data controller for, so should be removed)
 */
public class TppDataControllerFilterHelper {
    private static final Logger LOG = LoggerFactory.getLogger(TppDataControllerFilterHelper.class);

    private static final String FILTER_ORG_REG_AT_COLUMN = "IDOrganisationRegisteredAt";
    private static final String ORG_COLUMN = "IDOrganisation";
    private static final String PATIENT_ID_COLUMN = "IDPatient";
    private static final String REMOVED_DATA_COLUMN = "RemovedData";

    public static void filterFilesForSharedData(File orgDir, DataLayerI db) throws Exception {

        String orgId = orgDir.getName();
        File[] splitFiles = orgDir.listFiles();
        LOG.trace("Filtering shared data out of " + splitFiles.length + " files in " + orgDir);

        //save/update the split SRPatientRegistrationFile GMS orgs to the db
        for (File splitFile : splitFiles) {
            if (splitFile.getName().equalsIgnoreCase(TppConstants.PATIENT_REGISTRATION_FILE)) {

                LOG.debug("Found " + TppConstants.PATIENT_REGISTRATION_FILE + " file to save into db");
                FileInputStream fis = new FileInputStream(splitFile);
                BufferedInputStream bis = new BufferedInputStream(fis);
                InputStreamReader reader = new InputStreamReader(bis, Charset.forName(TppConstants.REQUIRED_CHARSET));
                CSVParser csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

                int count = 0;

                try {
                    Iterator<CSVRecord> csvIterator = csvParser.iterator();

                    //create a cached hashmap of Patient Gms orgs
                    HashMap<Integer, List<String>> cachedPatientGmsOrgs = new HashMap<>();

                    while (csvIterator.hasNext()) {
                        CSVRecord csvRecord = csvIterator.next();
                        String registrationStatus = csvRecord.get("RegistrationStatus");

                        // NOTE: IDOrganisationId is used to get the correct Orgs from patient registration status file,
                        // not the IDOrganisationRegisteredAt column
                        String gmsOrganisationId = csvRecord.get(ORG_COLUMN);
                        String patientId = csvRecord.get(PATIENT_ID_COLUMN);

                        // this is used on the rare occasion noted below
                        String organisationRegisteredAt = null;

                        //note this column isn't present in the test pack, so we need to check if the column exists
                        if (csvRecord.isMapped(FILTER_ORG_REG_AT_COLUMN)) {
                            organisationRegisteredAt = csvRecord.get(FILTER_ORG_REG_AT_COLUMN);
                        }

                        // save GMS registrations only. Using .contains as some status values contain both,
                        // i.e. GMS,Contraception for example
                        if (registrationStatus.contains("GMS")
                                && !Strings.isNullOrEmpty(gmsOrganisationId) && !Strings.isNullOrEmpty(patientId)) {

                            //don't bother adding the same organisation as the one being processed as that is the default
                            //check when filtering so no need to add it to the DB
                            if (gmsOrganisationId.equals(orgId)) {
                                continue;
                            }

                            Integer patId = Integer.parseInt(patientId);

                            //check if already added for that patient and org so do not attempt another db write
                            List<String> gmsOrgs;
                            if (cachedPatientGmsOrgs.containsKey(patId)) {

                                gmsOrgs = cachedPatientGmsOrgs.get(patId);
                                if (gmsOrgs.contains(gmsOrganisationId)) {
                                    continue;
                                }
                            } else {
                                gmsOrgs = new ArrayList<>();
                            }

                            TppOrganisationGmsRegistrationMap map = new TppOrganisationGmsRegistrationMap();
                            map.setOrganisationId(orgId);
                            map.setPatientId(patId);
                            map.setGmsOrganisationId(gmsOrganisationId);
                            db.addTppOrganisationGmsRegistrationMap(map);

                            //cache the patient Gms Orgs
                            gmsOrgs.add(gmsOrganisationId);
                            cachedPatientGmsOrgs.put(patId, gmsOrgs);
                            count++;

                            // if on the v rare occasion, the IDOrganisationRegisteredAt is different to the IDOrganisation
                            // and the IDOrganisationRegisteredAt is not part of the patient's GMS list, add it in to make
                            // sure SRPatientRegistration is filtered correctly later on.  First check it's not the
                            // same as the actual organisationId
                            if (organisationRegisteredAt != null
                                    && !organisationRegisteredAt.equals(orgId)) {

                                if (!gmsOrgs.contains(organisationRegisteredAt)) {

                                    map = new TppOrganisationGmsRegistrationMap();
                                    map.setOrganisationId(orgId);
                                    map.setPatientId(patId);
                                    map.setGmsOrganisationId(organisationRegisteredAt);
                                    db.addTppOrganisationGmsRegistrationMap(map);

                                    //cache the patient Gms Orgs
                                    gmsOrgs.add(organisationRegisteredAt);
                                    cachedPatientGmsOrgs.put(patId, gmsOrgs);
                                    count++;
                                }
                            }
                        }
                    }
                } finally {
                    csvParser.close();
                }

                //once we have found and processed the Patient Registration File we can break out this first part
                LOG.debug(count + " potentially new distinct Patient GMS organisations filed for OrganisationId: " + orgId);
                break;
            }
        }

        //get the latest Gms org db entries for this organisation and pop them into a hash map
        List<TppOrganisationGmsRegistrationMap> maps = db.getTppOrganisationGmsRegistrationMapsFromOrgId(orgId);

        HashMap<Integer, List<String>> filterPatientGmsOrgs = new HashMap<>();
        int count = 0;
        for (TppOrganisationGmsRegistrationMap map : maps) {
            String filterGmsOrgId = map.getGmsOrganisationId();
            Integer patientId = map.getPatientId();

            List<String> filterGmsOrgs;
            if (filterPatientGmsOrgs.containsKey(patientId)) {

                filterGmsOrgs = filterPatientGmsOrgs.get(patientId);
            } else {

                filterGmsOrgs = new ArrayList<>();
            }

            filterGmsOrgs.add(filterGmsOrgId);
            filterPatientGmsOrgs.put(patientId, filterGmsOrgs);
            count++;
        }

        LOG.debug(count + " Patient GMS organisation records found in DB for OrganisationId: " + orgId);

        // finally, filter each each split file
        for (File splitFile : splitFiles) {

            //create the csv parser input
            FileInputStream fis = new FileInputStream(splitFile);
            BufferedInputStream bis = new BufferedInputStream(fis);
            InputStreamReader reader = new InputStreamReader(bis, Charset.forName(TppConstants.REQUIRED_CHARSET));
            CSVParser csvParser = new CSVParser(reader, TppConstants.CSV_FORMAT.withHeader());

            CSVPrinter csvPrinter = null;

            try {

                Map<String, Integer> headerMap = csvParser.getHeaderMap();  //get the file header
                // convert the header map into an ordered String array, so we can check for the filter column
                // and then populate the column headers for the new CSV files later on
                String[] columnHeaders = new String[headerMap.size()];
                Iterator<String> headerIterator = headerMap.keySet().iterator();
                boolean hasfilterOrgColumn = false;
                boolean hasRemovedDataHeader = false;
                while (headerIterator.hasNext()) {
                    String headerName = headerIterator.next();
                    if (headerName.equalsIgnoreCase(FILTER_ORG_REG_AT_COLUMN))
                        hasfilterOrgColumn = true;

                    if (headerName.equalsIgnoreCase(REMOVED_DATA_COLUMN))
                        hasRemovedDataHeader = true;

                    int headerIndex = headerMap.get(headerName);
                    columnHeaders[headerIndex] = headerName;
                }

                //if the filter column header is not in this file, ignore and continue to next file
                if (!hasfilterOrgColumn) {
                    //LOG.debug("Filter column not found in file: "+splitFile.getName()+", skipping...");
                    csvParser.close();
                    continue;
                }

                //create a new temp file output from the splitFile to write the filtered records
                File splitFileTmp = new File(splitFile + ".tmp");
                FileOutputStream fos = new FileOutputStream(splitFileTmp);
                OutputStreamWriter osw = new OutputStreamWriter(fos, TppConstants.getCharset());
                BufferedWriter bufferedWriter = new BufferedWriter(osw);
                csvPrinter = new CSVPrinter(bufferedWriter, TppConstants.CSV_FORMAT.withHeader(columnHeaders));

                //create a new list of records based on the filtering process
                LOG.debug("Filtering file: " + splitFile);

                Iterator<CSVRecord> csvIterator = csvParser.iterator();
                count = 0;

                while (csvIterator.hasNext()) {
                    CSVRecord csvRecord = csvIterator.next();

                    // if the filter org column value matches the receiving org, include the data by default
                    // OR if it contains a removed data header header and it is a delete == 1, include the data
                    // OR is contained in the GMS organisation list for that patient, include the data
                    if (csvRecord.get(FILTER_ORG_REG_AT_COLUMN).equals(orgId)) {

                        csvPrinter.printRecord(csvRecord);
                        count++;

                    } else if (hasRemovedDataHeader && csvRecord.get(REMOVED_DATA_COLUMN).equals("1")) {
                        //RemovedData == 1 items have no registered organisation, so write anyway

                        csvPrinter.printRecord(csvRecord);
                        count++;

                    } else {
                        //otherwise, perform filtering on patient and additional GMS registered orgs

                        String patientId = csvRecord.get(PATIENT_ID_COLUMN);
                        Integer patId = Integer.parseInt(patientId);
                        String organisationRegisteredAt = csvRecord.get(FILTER_ORG_REG_AT_COLUMN);
                        if (filterPatientGmsOrgs.containsKey(patId)) {

                            List<String> filterGmsOrgs = filterPatientGmsOrgs.get(patId);
                            if (filterGmsOrgs.contains(organisationRegisteredAt)) {

                                csvPrinter.printRecord(csvRecord);
                                count++;
                            } //else {

                            //LOG.debug("Record excluded from filter: " + csvRecord.toString());
                            //}
                        } //else {
                        //LOG.error("Record has no other Patient GMS organisations to filter with: "+csvRecord.toString());
                        //}
                    }
                }

                LOG.debug("File filtering done: " + count + " records written for file: " + splitFile);

                //delete original file
                splitFile.delete();

                //rename the filtered tmp file to original filename
                splitFileTmp.renameTo(splitFile);
            } finally {
                //make sure everything is closed
                if (csvParser != null) {
                    csvParser.close();
                }
                if (csvPrinter != null) {
                    csvPrinter.close();
                }
            }
        }
    }
}
