package org.endeavourhealth.sftpreader.implementations.homerton.utility;

import org.apache.commons.csv.CSVFormat;

public class HomertonConstants {

    public static final CSVFormat CSV_FORMAT = CSVFormat.RFC4180.withHeader();

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";

    public static final String FILE_ID_PERSON = "person";
    public static final String FILE_ID_CONDITION = "condition";
    public static final String FILE_ID_PROCEDURE = "procedure";
}