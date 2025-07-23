// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva;

import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.openapi.OpenAPIConnection;
import com.boomi.connector.openapi.util.OpenAPIUtil;
import com.boomi.connector.veeva.util.ClientIDUtils;
import com.boomi.connector.veeva.util.HttpClientFactory;
import com.boomi.connector.veeva.util.LogUtils;
import com.boomi.connector.veeva.util.OpenAPIAction;
import com.boomi.connector.veeva.vault.api.VaultApiHandler;
import com.boomi.connector.veeva.vault.api.VaultApiHandlerFactory;
import com.boomi.util.LogUtil;
import com.boomi.util.URLUtil;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class VeevaConnection<C extends BrowseContext> extends OpenAPIConnection<C> implements VeevaBaseConnection {

    private static final Logger LOG = LogUtil.getLogger(VeevaConnection.class);
    private static final String AUTHORIZATION = "Authorization";

    private final VaultApiHandler _vaultApiHandler;

    VeevaConnection(C context) {
        this(context, VaultApiHandlerFactory.create(context));
    }

    // Constructor for testing purposes
    VeevaConnection(C context, VaultApiHandler vaultApiHandler) {
        super(context);
        _vaultApiHandler = vaultApiHandler;
    }

    @Override
    public String getSpec() {
        return "openapi.yaml";
    }

    /**
     * Returns the Veeva Vault API endpoint to interact with, including the API version in the path. The endpoint format
     * is https://{vault_domain_name}/api/{version}.
     *
     * @return string, the Veeva Vault API endpoint
     */
    @Override
    public URL getUrl() {
        return _vaultApiHandler.getVaultApiEndpoint();
    }

    /**
     * Requests a session id from Veeva and uses it to perform a get request to the provided path.
     *
     * @param client a http client
     * @param path   the get request uri
     * @return the http response
     * @throws IOException if an error occurs executing the request
     */
    public CloseableHttpResponse testConnection(CloseableHttpClient client, String path) throws IOException {
        CloseableHttpClient authClient = HttpClientFactory.createHttpClient(getContext());
        String sessionId = _vaultApiHandler.requestSessionId(authClient);

        // url returned by connection ends with a trailing slash and path string starts with a slash as well
        String url = URLUtil.trimTrailingSeparators(getUrl()) + path;

        HttpRequestBase httpRequest = new HttpGet(url);
        LOG.log(Level.FINE, LogUtils.requestMethodAndUriMessage(httpRequest));
        httpRequest.setHeader(AUTHORIZATION, sessionId);
        ClientIDUtils.setClientID(httpRequest, getContext());

        return client.execute(httpRequest);
    }

    /**
     * Returns a Session ID from the connector cache.
     *
     * @return string representing a Veeva Vault session
     */
    @Override
    public String getSessionIdFromCache() {
        return _vaultApiHandler.getSessionIdFromCache(getContext());
    }

    /**
     * Returns a key used to store Session IDs in the connector cache.
     *
     * @return string representing a key to cache Veeva Vault sessions
     */
    @Override
    public String getSessionCacheKey() {
        return _vaultApiHandler.getSessionCacheKey();
    }

    /**
     * Returns a list of Object Types. The "operationId" is used as the label. The object type ID contains the http
     * method and path in the following format {@code method::path}.
     *
     * @return the ObjectType List
     */
    @Override
    public List<ObjectType> getObjectTypes() {
        List<ObjectType> objects = new ArrayList<>();
        for (Map.Entry<String, PathItem> entrySet : getApi().getPaths().entrySet()) {
            PathItem path = entrySet.getValue();
            path.readOperationsMap().forEach((httpMethod, operation) -> {
                OpenAPIAction action = new OpenAPIAction(httpMethod, entrySet.getKey());
                objects.add(new ObjectType().withId(action.getId()).withLabel(operation.getOperationId()));
            });
        }
        return objects;
    }

    /**
     * Fetch the {@link Operation} based on the HTTP Method and Path value obtained from the given objectTypeId
     *
     * @param objectTypeId containing the selected HTTP Method and Path
     * @return the Operation
     */
    @Override
    protected Operation getOperation(String objectTypeId) {
        OpenAPIAction action = new OpenAPIAction(objectTypeId);
        return OpenAPIUtil.getOperation(getApi(), action.getMethod(), action.getPath());
    }
}