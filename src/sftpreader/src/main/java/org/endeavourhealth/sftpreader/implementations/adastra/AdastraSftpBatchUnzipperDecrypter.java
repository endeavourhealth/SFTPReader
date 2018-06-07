package org.endeavourhealth.sftpreader.implementations.adastra;

import org.endeavourhealth.sftpreader.implementations.SftpBatchUnzipperDecrypter;
import org.endeavourhealth.sftpreader.model.DataLayerI;
import org.endeavourhealth.sftpreader.model.db.Batch;
import org.endeavourhealth.sftpreader.model.db.DbConfiguration;
import org.endeavourhealth.sftpreader.model.db.DbInstanceEds;
import org.slf4j.LoggerFactory;

public class AdastraSftpBatchUnzipperDecrypter extends SftpBatchUnzipperDecrypter {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(AdastraSftpBatchUnzipperDecrypter.class);

    @Override
    public void unzipAndDecrypt(Batch batch, DbInstanceEds instanceConfiguration, DbConfiguration dbConfiguration, DataLayerI db) throws Exception {
        //no unzipping or decryption required
    }
}
