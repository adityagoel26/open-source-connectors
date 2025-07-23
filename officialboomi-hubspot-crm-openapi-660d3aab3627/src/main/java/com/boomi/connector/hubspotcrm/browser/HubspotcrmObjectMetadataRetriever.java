// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.hubspotcrm.browser;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.hubspotcrm.HubspotcrmConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.util.HubspotcrmConstant;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

import java.io.Closeable;
import java.util.logging.Logger;

/**
 * This class is responsible for retrieving the metadata associated to a hubspot-crm Object
 */
public class HubspotcrmObjectMetadataRetriever implements Closeable {

    private static final Logger LOG = LogUtil.getLogger(HubspotcrmObjectMetadataRetriever.class);

    private final CloseableHttpClient _httpClient;
    private final HubspotcrmConnection _connection;
    private static final String BASE_URL = "/crm/v3/properties/";
    private static final String SLASH = "/";
    private static final int PATCH_OBJECT_TYPE_INDEX = 2;
    private static final String SEARCH = "search";
    private static final String PATCH = "PATCH::";
    private static final String GET = "GET::";

    HubspotcrmObjectMetadataRetriever(CloseableHttpClient client, HubspotcrmConnection connection) {
        _httpClient = client;
        _connection = connection;
    }

    /**
     * Get a {@link JSONObject} representation of the metadata associated to the given hubspot-crm Object
     *
     * @param object the name of the hubspot-crm Object
     * @return the obtained metadata
     */
    public JSONObject getObjectMetadata(String object) {
        String[] parts = object.split(SLASH);
        if (parts[0].contains(PATCH) || parts[0].contains(GET) || object.contains(SEARCH)) {
            return executeGetHubspotcrmFieldMetadata(BASE_URL + parts[parts.length - PATCH_OBJECT_TYPE_INDEX]);
        }
        return executeGetHubspotcrmFieldMetadata(BASE_URL + parts[parts.length - 1]);
    }

    /**
     * Get a list of all hubspot-crm Object Names from the user Vault
     *
     * @return all the hubspot-crm Object Names
     */
    private JSONObject executeGetHubspotcrmFieldMetadata(String path) {
        CloseableHttpResponse response = null;
        try {
            response = ExecutionUtils.execute(HubspotcrmConstant.GET, path, _httpClient, null, _connection.getUrl(),
                    _connection.getHeaders(), LOG);
            return new JSONObject(EntityUtils.toString(response.getEntity()));
        } catch (Exception e) {
            LOG.log(java.util.logging.Level.SEVERE, "Error executing GET request: {0}", path);
            throw new ConnectorException("Failed to execute GET request: " + path, e);
        } finally {
            if (response != null) {
                IOUtil.closeQuietly(response);
            }
        }
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_httpClient);
    }
}