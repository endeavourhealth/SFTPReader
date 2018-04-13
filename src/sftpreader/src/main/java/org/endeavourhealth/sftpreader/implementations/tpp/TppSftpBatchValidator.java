package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.Validate;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.utility.FileHelper;
import org.endeavourhealth.common.utility.SlackHelper;
import org.endeavourhealth.sftpreader.DataLayer;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.db.*;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.util.*;

public class TppSftpBatchValidator extends SftpBatchValidator {

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayer db) throws SftpValidationException {

        //add checks as required

        return true;
    }

}
