package org.endeavourhealth.sftpreader.implementations;

import com.fasterxml.jackson.databind.JsonNode;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.sftpreader.model.DataLayerI;

import org.endeavourhealth.sftpreader.model.db.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class SftpBatchSplitter {
    private static final Logger LOG = LoggerFactory.getLogger(SftpBatchSplitter.class);

    public abstract List<BatchSplit> splitBatch(Batch batch,
                                                Batch lastCompleteBatch,
                                                DataLayerI db,
                                                DbInstanceEds instanceConfiguration,
                                                DbConfiguration dbConfiguration) throws Exception;

    /**
     * returns a list of ODS codes that we shouldn't try to process data for
     * used when we have a publisher that's closed or similar, but the feeds are still active for
     * other publishers, and we want to stop sending admin data through.
     */
    public static Set<String> findOdsCodesThatShouldBeIgnored(String configurationId) throws Exception {

        Set<String> ret = new HashSet<>();

        JsonNode json = ConfigManager.getConfigurationAsJson("ignored_publishers");
        if (json == null) {
            LOG.debug("Failed to find config for ignored_publishers");
            return ret;
        }

        if (!json.has(configurationId)) {
            return ret;
        }

        JsonNode subNode = json.get(configurationId);
        for (int i=0; i<subNode.size(); i++) {
            String odsCode = subNode.get(i).asText();
            ret.add(odsCode);
        }

        return ret;
    }
}
