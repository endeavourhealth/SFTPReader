package org.endeavourhealth.sftpreader.implementations.tpp.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

import java.nio.charset.Charset;

public class TppConstants {

    public static final String ORGANISATION_FILE = "SROrganisation.csv";
    public static final String MANIFEST_FILE = "SRManifest.csv";
    public static final String MAPPING_FILE = "SRMapping.csv";
    public static final String MAPPING_GROUP_FILE = "SRMappingGroup.csv";
    public static final String PATIENT_REGISTRATION_FILE = "SRPatientRegistration.csv";
    public static final String PATIENT_FILE = "SRPatient.csv";
    public static final String CODE_FILE = "SRCode.csv";

    //note these are file TYPES, not file names
    public static final String PATIENT_FILE_TYPE = "Patient";
    public static final String CODE_FILE_TYPE = "Code";
    public static final String MANIFEST_FILE_TYPE = "Manifest";

    public static final String REQUIRED_CHARSET = "Cp1252";

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL);

    //Column unique identifier for SR Code file
    public static final String COL_ROW_IDENTIFIER_TPP = "RowIdentifier";



    public static Charset getCharset() {
        return Charset.forName(TppConstants.REQUIRED_CHARSET);
    }
}
