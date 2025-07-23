// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.util.BaseConnection;
import com.boomi.salesforce.rest.authenticator.TokenManager;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.salesforce.rest.request.AuthenticationRequestExecutor;
import com.boomi.salesforce.rest.request.ProxyHelper;
import com.boomi.salesforce.rest.request.RequestBuilder;
import com.boomi.salesforce.rest.request.RequestExecutor;
import com.boomi.salesforce.rest.request.RequestHandler;
import com.boomi.salesforce.rest.request.SFURIBuilder;
import com.boomi.util.IOUtil;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;

import java.io.Closeable;
import java.util.logging.Logger;

@SuppressWarnings("rawtypes")
public class SFRestConnection extends BaseConnection implements Closeable {

    private ConnectionProperties _connectionProperties;
    private OperationProperties _operationProperties;
    private CloseableHttpClient _client;
    private RequestHandler _requestHandler;

    @SuppressWarnings("unchecked")
    public SFRestConnection(BrowseContext context) {
        super(context);
    }

    /**
     * Initializes the ConnectionManager
     *
     * @param logger will be used to log messages for the running operation
     */
    public void initialize(Logger logger) {
        initProperties(logger);
        initConnection();
    }

    public void initialize() {
        initialize(null);
    }

    private void initProperties(Logger logger) {
        _operationProperties = new OperationProperties(((BrowseContext) getContext()).getOperationProperties(),
                ((BrowseContext) getContext()).getCustomOperationType());

        _connectionProperties = new ConnectionProperties(getContext().getConnectionProperties(),
                getContext().getConnectorCache(), logger);
        _connectionProperties.logFine(
                "Loading Salesforce Connection and Operation settings and establishing connection.");
    }

    void initConnection() {
        _client = new ProxyHelper(getContext()).configure(HttpClientBuilder.create()).build();
        SFURIBuilder salesforceSFURIBuilder = new SFURIBuilder(_connectionProperties.getURL(),
                _connectionProperties.getAuthenticationUrl());
        RequestBuilder requestBuilder = new RequestBuilder(salesforceSFURIBuilder, _operationProperties);
        TokenManager tokenManager = TokenManager.getTokenManager(_connectionProperties, requestBuilder,
                new AuthenticationRequestExecutor(_client));
        RequestExecutor requestExecutor = new RequestExecutor(_client, tokenManager, _connectionProperties,
                _operationProperties);
        _requestHandler = new RequestHandler(requestBuilder, requestExecutor);
        // test connection to validate session activity, avoid long waits, and input reset errors
        _requestHandler.testConnection();
    }

    public OperationProperties getOperationProperties() {
        return _operationProperties;
    }

    public ConnectionProperties getConnectionProperties() {
        return _connectionProperties;
    }

    public RequestHandler getRequestHandler() {
        return _requestHandler;
    }

    @Override
    public void close() {
        if (_connectionProperties != null) {
            _connectionProperties.logInfo(_operationProperties.getAPILimit());
            _connectionProperties.logFine("Closing Salesforce connector.");
        }
        IOUtil.closeQuietly(_client);
    }

    /**
     * Used for testing purposes
     */
    void setClient(CloseableHttpClient client) {
        _client = client;
    }
}
