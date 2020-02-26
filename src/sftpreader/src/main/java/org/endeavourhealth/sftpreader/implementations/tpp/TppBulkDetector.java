package org.endeavourhealth.sftpreader.implementations.tpp;

import org.apache.commons.io.FilenameUtils;
import org.endeavourhealth.sftpreader.implementations.SftpBulkDetector;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.ManifestRecord;
import org.endeavourhealth.sftpreader.implementations.tpp.utility.TppConstants;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchSplit;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

import java.io.File;
import java.util.List;

public class TppBulkDetector extends SftpBulkDetector {

    /**
     * detect a TPP bulk by checking the SRManifest file, which has a flag
     */
    @Override
    public boolean isBulkExtract(Batch batch, BatchSplit batchSplit, DataLayerI db,
                                 DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration) throws Exception {

        //the SRManifest file will still be in our temp storage
        String tempDir = instanceConfiguration.getTempDirectory(); //e.g. c:\temp
        String configurationDir = dbConfiguration.getLocalRootPath(); //e.g. TPP_TEST
        String splitRelativePath = batchSplit.getLocalRelativePath(); //e.g. 2017-04-27T09.08.00\Split\HSCIC6

        String sourceTempDir = FilenameUtils.concat(tempDir, configurationDir);
        sourceTempDir = FilenameUtils.concat(sourceTempDir, splitRelativePath);

        String manifestPath = FilenameUtils.concat(sourceTempDir, TppConstants.MANIFEST_FILE);
        File f = new File(manifestPath);
        if (!f.exists()) {
            throw new Exception("Failed to find manifest file " + f);
        }

        //we know that a manifest may contain a mix of bulks and deltas, but for the sake of
        //simplicity treat it as a bulk if we have SRPatient and SRCode bulk files
        boolean patientBulk = false;
        boolean codeBulk = false;

        List<ManifestRecord> records = ManifestRecord.readManifestFile(f);
        for (ManifestRecord record: records) {
            String fileName = record.getFileNameWithExtension();
            if (fileName.equals(TppConstants.PATIENT_FILE)) {
                patientBulk = !record.isDelta();

            } else if (fileName.equals(TppConstants.CODE_FILE)) {
                codeBulk = !record.isDelta();
            }
        }

        if (patientBulk && codeBulk) {
            return true;

        } else {
            return false;
        }
    }
}
