package org.endeavourhealth.sftpreader.sender;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.endeavourhealth.common.security.keycloak.client.KeycloakClient;
import org.endeavourhealth.core.database.dal.audit.models.HeaderKeys;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class EdsSender {

    private static final String EDS_ENVELOPE_TEMPLATE_FILENAME = "EdsEnvelopeTemplate.xml";
    public static final int HTTP_REQUEST_TIMEOUT_MILLIS = 30 * 1000;

    private EdsSender() {
    }

    public static String buildEnvelope(UUID messageId, String organisationId, String sourceSoftware, String sourceSoftwareVersion, String payload) throws IOException {

        String edsEnvelope = Resources.toString(Resources.getResource(EDS_ENVELOPE_TEMPLATE_FILENAME), Charsets.UTF_8);

        Map<String, String> test = new HashMap<String, String>() {
            {
                put("{{message-id}}", messageId.toString());
                put("{{timestamp}}", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
                put("{{source-name}}", organisationId);
                put("{{source-software}}", sourceSoftware);
                put("{{source-version}}", sourceSoftwareVersion);
                put("{{source-endpoint}}", "");
                put("{{payload-id}}", UUID.randomUUID().toString());
                put("{{payload-type}}", "application/base64");
                put("{{payload-base64}}", Base64.getEncoder().encodeToString(payload.getBytes()));
            }
        };

        for (Map.Entry<String,String> entry : test.entrySet())
            edsEnvelope = edsEnvelope.replace(entry.getKey(), entry.getValue());

        return edsEnvelope;
    }

    public static EdsSenderResponse notifyEds(String edsUrl, boolean useKeycloak, String outboundMessage,
                                              boolean isBulk, boolean hasPatientData, Long fileTotalSize,
                                              Date extractDate, Date extractCutoff) throws EdsSenderHttpErrorResponseException, IOException
    {
        RequestConfig requestConfig = RequestConfig
                .custom()
                .setConnectTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                .setSocketTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                .setConnectionRequestTimeout(HTTP_REQUEST_TIMEOUT_MILLIS)
                .build();

        try (CloseableHttpClient httpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()) {
            HttpPost httpPost = new HttpPost(edsUrl);

            //add the headers to our HTTP POST
            if (useKeycloak) {
                httpPost.addHeader(KeycloakClient.instance().getAuthorizationHeader());
            }
            if (isBulk) {
                httpPost.addHeader(HeaderKeys.IsBulk, "true");
            }
            if (!hasPatientData) {
                httpPost.addHeader(HeaderKeys.HasPatientData, "false"); //only add this flag if NO patient data is found
            }
            if (fileTotalSize != null) {
                httpPost.addHeader(HeaderKeys.TotalFileSize, "" + fileTotalSize);
            }
            if (extractDate != null) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(HeaderKeys.DATE_FORMAT);
                httpPost.addHeader(HeaderKeys.ExtractDate, dateFormat.format(extractDate));
            }
            if (extractCutoff != null) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(HeaderKeys.DATE_FORMAT);
                httpPost.addHeader(HeaderKeys.ExtractCutoff, dateFormat.format(extractCutoff));
            }

            httpPost.addHeader("Content-Type", "text/xml");
            httpPost.setEntity(new ByteArrayEntity(outboundMessage.getBytes()));

            HttpResponse response = httpClient.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            String statusLine = response.getStatusLine().toString();
            String responseBody = null;

            if (response.getEntity() != null)
                if (response.getEntity().getContent() != null)
                    responseBody = IOUtils.toString(response.getEntity().getContent(), "UTF-8");

            EdsSenderResponse edsSenderResponse = new EdsSenderResponse()
                    .setHttpStatusCode(statusCode)
                    .setStatusLine(statusLine)
                    .setResponseBody(responseBody);

            if (edsSenderResponse.getHttpStatusCode() != HttpStatus.SC_OK) {
                String message = "Error received from EDS: " + edsSenderResponse.getStatusLine() + " > " + getFirstTwoLines(edsSenderResponse.getResponseBody());
                throw new EdsSenderHttpErrorResponseException(message, edsSenderResponse);
            }

            return edsSenderResponse;
        }
    }

    private static String getFirstTwoLines(String message) {
        if (message == null)
            return null;

        String[] lines = StringUtils.trim(message).split("\\r?\\n");

        String result = "";

        if (lines.length > 0)
            result += lines[0];

        if (lines.length > 1)
            result += "\r\n" + lines[1];

        if (lines.length > 2)
            result += "...";

        return result;
    }
}
