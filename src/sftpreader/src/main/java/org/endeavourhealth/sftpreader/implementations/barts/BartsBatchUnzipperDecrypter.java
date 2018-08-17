package org.endeavourhealth.sftpreader.implementations.barts;

import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;

public class BartsBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {
        //no unzipping or decryption required
    }
}
