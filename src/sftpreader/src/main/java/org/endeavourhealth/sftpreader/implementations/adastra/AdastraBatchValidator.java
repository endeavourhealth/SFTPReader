package org.endeavourhealth.sftpreader.implementations.adastra;

import org.apache.commons.lang3.Validate;
import org.endeavourhealth.sftpreader.implementations.SftpBatchValidator;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.BatchFile;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.endeavourhealth.sftpreader.model.exceptions.SftpValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdastraBatchValidator extends SftpBatchValidator {

    private static final Logger LOG = LoggerFactory.getLogger(AdastraBatchValidator.class);

    @Override
    public boolean validateBatch(Batch incompleteBatch, Batch lastCompleteBatch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws SftpValidationException {

        Validate.notNull(incompleteBatch, "incompleteBatch is null");
        Validate.notNull(dbConfiguration, "dbConfiguration is null");
        Validate.notNull(dbConfiguration.getInterfaceFileTypes(), "dbConfiguration.interfaceFileTypes is null");
        Validate.notEmpty(dbConfiguration.getInterfaceFileTypes(), "No interface file types configured");

        //a batch for an Adastra extract is made up of 11 files (v2)
        int fileCount = incompleteBatch.getBatchFiles().size();
        if (fileCount != 11) {
            throw new SftpValidationException("Incorrect number of files ("+Integer.toString(fileCount)+") in batch. Batch identifier = " + incompleteBatch.getBatchIdentifier());
        }

        //validate that all files in the batch have the same org code - Advanced have said they'll
        //send each services data in a single batch with the same date in the filename, but do this check
        //for extra protection
        String orgCode = null;
        for (BatchFile f: incompleteBatch.getBatchFiles()) {
            String fileName = f.getFilename();
            String fileOrgCode = AdastraBatchSplitter.getFilenameOrgCode(fileName);
            if (orgCode == null) {
                orgCode = fileOrgCode;
            } else if (!orgCode.equals(fileOrgCode)) {
                throw new SftpValidationException("Inconsistent org code in batch " + incompleteBatch.getBatchId() + " file names " + orgCode + " vs " + fileOrgCode);
            }
        }

        return true;
    }

}
