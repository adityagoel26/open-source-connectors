// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.IOUtil;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpHost;

/**
 * Responsible for executing requests and closing the request entity
 */
public class AuthenticationRequestExecutor {

    private final CloseableHttpClient _httpClient;

    /**
     * @param httpClient
     */
    public AuthenticationRequestExecutor(CloseableHttpClient httpClient) {
        _httpClient = httpClient;
    }

    /**
     * Executes an authentication request. Closes the request body stream
     *
     * @param request the request to be executed
     * @return response the response if it was successful
     * @throws ConnectorException if any error, or if the response returned an error, exception contains the response
     *                            error message
     */
    public ClassicHttpResponse executeAuthenticate(ClassicHttpRequest request) {
        ClassicHttpResponse response = null;
        try {
            response = _httpClient.executeOpen(new HttpHost(request.getScheme(), request.getAuthority()), request,
                    null);
            ResponseHandler responseHandler = new ResponseHandler(response);

            if (responseHandler.isAnyError()) {
                throw new ConnectorException(
                        responseHandler.getAuthenticationErrorMessage() + " Error code: " + response.getCode());
            }
        } catch (Exception e) {
            IOUtil.closeQuietly(response);
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            // Closes the request body entity after sending the request
            IOUtil.closeQuietly(request.getEntity());
        }
        return response;
    }
}
