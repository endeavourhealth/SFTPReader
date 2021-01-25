package org.endeavourhealth.sftpreader.implementations.tpp;

import org.endeavourhealth.sftpreader.implementations.SftpBatchDateDetector;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.vision.VisionFilenameParser;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;

public class TppDateDetector extends SftpBatchDateDetector {

    @Override
    public Date detectExtractDate(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {
        //the TPP extract date is part of the filename, which is what we use for the batch identifier, so parse that back to a date
        String batchIdentifier = batch.getBatchIdentifier();
        LocalDateTime localDateTime = TppFilenameParser.parseBatchIdentifier(batchIdentifier);
        Date dateTime = java.sql.Timestamp.valueOf(localDateTime);
        return dateTime;
    }

    @Override
    public Date detectExtractCutoff(Batch batch, DataLayerI db, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //we know (see SD-319) that TPP has a variable cut-off time for each extract, but the SRManifest file gives that information
        String tempDir = TppBatchValidator.getTempDirectoryPathForBatch(instanceConfiguration, dbConfiguration, batch);
        List<ManifestRecord> manifestRecords = TppBatchValidator.readManifestFromDirectory(tempDir);

        for (ManifestRecord record: manifestRecords) {

            //there's some kind of bug in SystmOne where the manifest record for this one file seems to be
            //without either start or end date most of the time. Since we don't actually process this file, I think
            //it reasonable to just skip this file for this validation, since we are still validating 100+ other files.
            String file = record.getFileNameWithoutExtension();
            if (file.equals(TppBatchValidator.APPOINTMENT_ATTENDEES)) {
                continue;
            }

            //the TPPBatchValidator makes sure that the end date present and is the same for all manifest records, so
            //we can just use the date from any record (except the one we skip above)
            return record.getDateTo();
        }

        return null;
    }
}
