package org.endeavourhealth.sftpreader.implementations.adastra.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class AdastraConstants {

    //note the Adastra files don't include headers, but AdastraHelper has a fn to provide those
    public static final CSVFormat CSV_FORMAT = CSVFormat.DEFAULT.withDelimiter('|').withQuoteMode(QuoteMode.ALL);

    public static final String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss"; //Adastra actually give MS too, but ignore that

    public static final String FILE_ID_CASE = "CASE";
    public static final String FILE_ID_CASE_QUESTIONS = "CASEQUESTIONS";
    public static final String FILE_ID_CLINICAL_CODES = "CLINICALCODES";
    public static final String FILE_ID_CONSULTATION = "CONSULTATION";
    public static final String FILE_ID_ELECTRONIC_PRESCRIPTIONS = "ELECTRONICPRESCRIPTIONS";
    public static final String FILE_ID_Electronic_Prescriptions = "ElectronicPrescriptions";
    public static final String FILE_ID_NOTES = "NOTES";
    public static final String FILE_ID_OUTCOMES = "OUTCOMES";
    public static final String FILE_ID_PATIENT = "PATIENT";
    public static final String FILE_ID_PRESCRIPTIONS = "PRESCRIPTIONS";
    public static final String FILE_ID_PROVIDER = "PROVIDER";
    public static final String FILE_ID_USERS = "USERS";
}
