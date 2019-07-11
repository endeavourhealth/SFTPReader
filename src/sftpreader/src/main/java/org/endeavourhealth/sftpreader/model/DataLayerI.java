package org.endeavourhealth.sftpreader.model;

import org.endeavourhealth.common.postgres.PgResultSet;
import org.endeavourhealth.common.postgres.PgStoredProc;
import org.endeavourhealth.common.postgres.PgStoredProcException;
import org.endeavourhealth.common.utility.StreamExtension;
import org.endeavourhealth.sftpreader.SftpFile;
import org.endeavourhealth.sftpreader.model.db.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

public interface DataLayerI {

    DbInstance getInstanceConfiguration(String instanceName, String hostname) throws Exception;
    DbConfiguration getConfiguration(String configurationId) throws Exception;



    AddFileResult addFile(String configurationId, SftpFile sftpFile) throws Exception;

    void setFileAsDownloaded(SftpFile batchFile) throws Exception;
    void setFileAsDeleted(BatchFile batchFile) throws Exception;

    boolean addUnknownFile(String configurationId, SftpFile batchFile) throws Exception;

    List<Batch> getIncompleteBatches(String configurationId) throws Exception;

    Batch getLastCompleteBatch(String configurationId) throws Exception;
    List<Batch> getAllBatches(String configurationId) throws Exception;

    List<BatchSplit> getUnnotifiedBatchSplits(String configurationId) throws Exception;

    List<UnknownFile> getUnknownFiles(String configurationId) throws Exception;

    void setBatchAsComplete(Batch batch) throws Exception;
    void setBatchSequenceNumber(Batch batch, Integer sequenceNumber) throws Exception;

    void addBatchNotification(int batchId, int batchSplitId, String configurationId, UUID messageId, String outboundMessage, String inboundMessage, boolean wasSuccess, String errorText) throws Exception;
    void addBatchSplit(BatchSplit batchSplit) throws Exception;
    void deleteBatchSplits(Batch batch) throws Exception;
    List<BatchSplit> getBatchSplitsForBatch(int queryBatchId) throws Exception;

    void addEmisOrganisationMap(EmisOrganisationMap mapping) throws Exception;
    List<EmisOrganisationMap> getEmisOrganisationMapsForOdsCode(String odsCode) throws Exception;
    EmisOrganisationMap getEmisOrganisationMap(String guid) throws Exception;

    void addTppOrganisationMap(TppOrganisationMap mapping) throws Exception;
    TppOrganisationMap getTppOrgNameFromOdsCode(String queryOdsCode) throws Exception;

    ConfigurationLockI createConfigurationLock(String lockName) throws Exception;

    List<String> getNotifiedMessages(BatchSplit batchSplit) throws Exception;
}
