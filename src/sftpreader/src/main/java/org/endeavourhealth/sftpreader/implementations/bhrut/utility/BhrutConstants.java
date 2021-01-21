package org.endeavourhealth.sftpreader.implementations.bhrut.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class BhrutConstants {

    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withHeader().withQuoteMode(QuoteMode.MINIMAL);

    public static final String DATE_TIME_FORMAT = "dd/MM/yyyy HH:mm:ss";

    public static final String FILE_ID_PMI = "PMI";
    public static final String FILE_ID_AE_ATTENDANCES = "AE_ATTENDANCES";
    public static final String FILE_ID_INPATIENT_SPELLS = "INPATIENT_SPELLS";
}
