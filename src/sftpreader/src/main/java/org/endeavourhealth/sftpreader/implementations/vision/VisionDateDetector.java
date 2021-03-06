package org.endeavourhealth.sftpreader.implementations.vision;

import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.emis.EmisFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

public class VisionDateDetector extends SftpBatchDateDetector {

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //the Vision extract date is part of the filename, which is what we use for the batch identifier, so parse that back to a date
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = VisionFilenameParser.parseBatchIdentifier(batchIdentifier);
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //we know (see SD-319) that Vision cut-off for extracts at midnight. So work back from the extract date to find the cutoff.
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = VisionFilenameParser.parseBatchIdentifier(batchIdentifier);
        LocalDate localDate = localDateTime.toLocalDate(); //convert to just DATE
        localDateTime = localDate.atStartOfDay(); //add midnight to date
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }
}
