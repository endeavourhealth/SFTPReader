package org.endeavourhealth.sftpreader.sender;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.endeavourhealth.common.cache.ObjectMapperPool;
import org.endeavourhealth.common.config.ConfigManager;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.common.utility.ExpiringCache;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class DpaCheck {
    private static final Logger LOG = LoggerFactory.getLogger(DpaCheck.class);

    /**
     * the Messaging API has an endpoint for checking if a DPA exists. This fn calls that to check.
     */
    public static boolean checkForDpa(String organisationId, boolean useKeycloak, String messagingApiUrl) throws Exception {

        //we've already got config telling us the Messaging API endpoint for the normal posting of new
        //data, so just derive the API we need from that
        //starting from:
        //http://localhost:8081/api/PostMessageAsync
        //https://n3messageapi.discoverydataservice.net/machine-api/PostMessageAsync

        int slashIx = messagingApiUrl.lastIndexOf("/");
        if (slashIx == -1) {
            throw new Exception("Unable to determine DPA endpoint from URL [" + messagingApiUrl + "]");
        }
        String prefix = messagingApiUrl.substring(0, slashIx);
        String dpaUrl = prefix + "/dsm/hasDPA/" + organisationId;
        LOG.trace("DPA check URL: " + dpaUrl);

        //use the same timeouts as used by the regular calls to the Messaging API
        RequestConfig requestConfig = RequestConfig
                .custom()
                .setConnectTimeout(EdsSender.HTTP_REQUEST_TIMEOUT_MILLIS)
                .setSocketTimeout(EdsSender.HTTP_REQUEST_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(EdsSender.HTTP_REQUEST_TIMEOUT_MILLIS)
                .build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()) {

            HttpGet httpGet = new HttpGet(dpaUrl);

            //add the headers to our HTTP POST
            if (useKeycloak) {
                httpGet.addHeader(KeycloakClient.instance().getAuthorizationHeader());
            }

            HttpResponse response = httpClient.execute(httpGet);

            int statusCode = response.getStatusLine().getStatusCode();
            //String statusLine = response.getStatusLine().toString();
            String responseBody = null;

            if (response.getEntity() != null) {
                if (response.getEntity().getContent() != null) {
                    responseBody = IOUtils.toString(response.getEntity().getContent(), "UTF-8");
                }
            }

            if (statusCode == HttpStatus.SC_OK) {
                JsonNode jsonNode = ObjectMapperPool.getInstance().readTree(responseBody);
                boolean hasDpa = jsonNode.get("hasDPA").asBoolean();
                LOG.trace("DPA check for " + organisationId + " -> " + hasDpa);
                return hasDpa;

            } else {
                LOG.error("Failed to check DPA at: " + dpaUrl);
                LOG.error("Received response: " + responseBody);
                throw new Exception("HTTP " + statusCode + " checking for DPA for " + organisationId);
            }
        }
    }
}
