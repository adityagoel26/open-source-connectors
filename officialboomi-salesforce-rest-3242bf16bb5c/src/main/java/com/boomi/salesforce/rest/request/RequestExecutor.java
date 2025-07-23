// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.salesforce.rest.authenticator.TokenManager;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.message.BasicHeader;

import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Responsible for executing requests and closing the request entity
 */
public class RequestExecutor {

    private static final Logger LOG = LogUtil.getLogger(RequestExecutor.class);

    private final TokenManager _tokenManager;
    private final ConnectionProperties _connectionProperties;
    private final OperationProperties _operationProperties;
    private final CloseableHttpClient _httpClient;

    /**
     * @param tokenManager TokenManager instance responsible for authentication
     */
    public RequestExecutor(CloseableHttpClient httpClient, TokenManager tokenManager,
                           ConnectionProperties connectionProperties, OperationProperties operationProperties) {
        _httpClient = httpClient;
        _tokenManager = tokenManager;
        _connectionProperties = connectionProperties;
        _operationProperties = operationProperties;
    }

    /**
     * Executes a request and retry it with new sessionID if session expired response was given. Closes the request body
     * stream
     *
     * @param request the request to be executed
     * @return response the response if it was successful
     * @throws ConnectorException if any error, or if the response returned an error, exception contains the response
     *                            error message
     */
    public ClassicHttpResponse execute(ClassicHttpRequest request) throws ConnectorException {
        ClassicHttpResponse response = null;
        try {
            HttpHost target = new HttpHost(request.getScheme(), request.getAuthority());
            logRequest(request);
            response = _httpClient.executeOpen(target, request, null);
            ResponseHandler responseHandler = new ResponseHandler(response);
            // Generates a new sessionID if session expired
            if (responseHandler.isSessionExpired()) {
                prepareRetryRequest(request, response);
                logRequest(request);
                response = _httpClient.executeOpen(target, request, null);
                responseHandler = new ResponseHandler(response);
            }

            if (responseHandler.isAnyError()) {
                throw new ConnectorException(String.valueOf(response.getCode()),
                        responseHandler.getErrorMessage() + " Error code: " + response.getCode());
            }
        } catch (Exception e) {
            IOUtil.closeQuietly(response);
            if (e instanceof ConnectorException) {
                throw (ConnectorException) e;
            }
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            // Closes the request body entity after sending the request
            IOUtil.closeQuietly(request.getEntity());
        }
        return response;
    }

    /**
     * Write the request URL and method to the container and process log.
     *
     * @param request to extract the URL and method
     */
    private void logRequest(ClassicHttpRequest request) throws URISyntaxException {
        String url = request.getUri().toString();
        String requestInfo = MessageFormat.format("Executing request: [{0}] {1}", request.getMethod(), url);

        LOG.log(Level.FINE, requestInfo);
        _connectionProperties.logInfo(requestInfo);
    }

    private void prepareRetryRequest(ClassicHttpRequest request, ClassicHttpResponse response) {
        _connectionProperties.logWarning("Session expired or unauthorized, generating new session...");

        // closes the response and execute the request again with a new sessionID
        IOUtil.closeQuietly(response);

        request.setHeader(Constants.AUTHORIZATION_REQUEST,
                          Constants.BEARER_REQUEST + _tokenManager.generateAccessToken());

        HttpEntity requestEntity = request.getEntity();
        try {
            if (requestEntity != null) {
                requestEntity.getContent().reset();
            }
        } catch (Exception e) {
            // tries to reset content when retry the same request with a new session Id
            _connectionProperties.logWarning(() -> "Failed to resend the request body: " + e.getMessage(), e);
        }
    }

    /**
     * Executes the request and returns the content response
     *
     * @param request      the request to be executed
     * @param errorMessage the error message to be shown if any error occurred
     * @return ClassicHttpResponse of the response body
     * @throws ConnectorException if any error occurred
     */
    public ClassicHttpResponse executeStream(ClassicHttpRequest request, String errorMessage) {
        fillRESTHeaders(request);

        ClassicHttpResponse response = null;
        try {
            response = execute(request);

            storeAPIInfo(response);

            return response;
        } catch (Exception e) {
            IOUtil.closeQuietly(response);
            throw new ConnectorException(
                    "[Errors occurred while executing " + errorMessage + " request] " + e.getMessage(), e);
        }
    }

    /**
     * Executes request with no-content body
     *
     * @param request      the request to be executed
     * @param errorMessage the error message to be shown if any error occurred
     * @throws ConnectorException if any error occurred
     */
    public void executeVoid(ClassicHttpRequest request, String errorMessage) {
        ClassicHttpResponse response = executeStream(request, errorMessage);
        IOUtil.closeQuietly(response);
    }

    private void storeAPIInfo(ClassicHttpResponse response) {
        String apiLimit = new ResponseHandler(response).getAPIInfo();
        if (StringUtil.isNotBlank(apiLimit)) {
            _operationProperties.setAPILimit(apiLimit);
        }
    }

    private void fillRESTHeaders(ClassicHttpRequest request) {
        request.addHeader(new BasicHeader(Constants.AUTHORIZATION_REQUEST,
                Constants.BEARER_REQUEST + _tokenManager.getAccessToken()));
        _operationProperties.getRestHeaders().forEach(request::addHeader);
    }

}
