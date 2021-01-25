package org.endeavourhealth.sftpreader.implementations.emisCustom.utility;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.QuoteMode;

public class EmisCustomConstants {

    public static final CSVFormat CSV_FORMAT = CSVFormat.TDF
            .withEscape((Character)null)
            .withQuote((Character)null)
            .withQuoteMode(QuoteMode.MINIMAL); //ideally want Quote Mode NONE, but validation in the library means we need to use this;

}
