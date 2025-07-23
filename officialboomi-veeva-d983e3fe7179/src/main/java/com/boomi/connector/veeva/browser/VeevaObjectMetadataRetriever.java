// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva.browser;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.veeva.VeevaBaseConnection;
import com.boomi.connector.veeva.util.ExecutionUtils;
import com.boomi.util.IOUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.Closeable;

/**
 * This class is in charge of retrieving Veeva Object Metadata
 */
public class VeevaObjectMetadataRetriever implements Closeable {

    private final CloseableHttpClient _httpClient;
    private final VeevaBaseConnection _connection;

    VeevaObjectMetadataRetriever(CloseableHttpClient client, VeevaBaseConnection connection) {
        _httpClient = client;
        _connection = connection;
    }

    /**
     * Get a {@link JSONObject} representation of the metadata associated to the Document Veeva Object
     *
     * @return the obtained metadata
     */
    public JSONObject getDocumentsMetadata() {
        return executeGetVeevaFieldMetadata("/metadata/objects/documents/properties");
    }

    /**
     * Get a {@link JSONObject} representation of the metadata associated to the given Veeva Object
     *
     * @param veevaObject the name of the Veeva Object
     * @return the obtained metadata
     */
    public JSONObject getObjectMetadata(String veevaObject) {
        return executeGetVeevaFieldMetadata("/metadata/vobjects/" + veevaObject);
    }

    /**
     * Get a list of all Veeva Object Names from the user Vault
     *
     * @return all the Veeva Object Names
     */
    JSONObject getAllObjects() {
        return executeGetVeevaFieldMetadata("/metadata/vobjects");
    }

    private JSONObject executeGetVeevaFieldMetadata(String path) {
        CloseableHttpResponse response = null;
        try {
            response = ExecutionUtils.execute("GET", path, _httpClient, null, _connection);

            JSONObject jsonResponse = new JSONObject(new JSONTokener(response.getEntity().getContent()));
            if (!"SUCCESS".equalsIgnoreCase(jsonResponse.getString("responseStatus"))) {
                throw new ConnectorException(jsonResponse.toString());
            }
            return jsonResponse;
        } catch (Exception e) {
            throw new ConnectorException("Unable to browse ", e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(response);
        }
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_httpClient);
    }
}
