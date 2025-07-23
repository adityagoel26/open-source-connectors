// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.retry;

import com.boomi.common.apache.http.response.HttpResult;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.openapi.OpenAPIRestClient;
import com.boomi.connector.veeva.VeevaOperationConnection;
import com.boomi.connector.veeva.util.LogUtils;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.retry.RetryStrategy;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.RequestBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * RestClient which handles Veeva request executions that can be retried. This custom implementation was created
 * considering that, when a failure occurs, Veeva returns HTTP 200 responses and the failure reason is included in the
 * payload. As a consequence, in order to catch a particular failure (e.g.: unauthorized due to an invalid/expired
 * session id), it is necessary to parse the response content.
 * <p>
 * The content stream included in the {@link HttpResult} can only be consumed once. Therefore, this client copy the
 * stream into a temp stream that can be reset, i.e.: the temp stream is consumed once to check whether the response is
 * successful, if that is the case it is reset, and then it is consumed again as operation result.
 * <p>
 * By the time this was developed, Veeva has been adding a response header 'X-VaultAPI-Status', with the response
 * status, to certain API endpoints. Using the latter and other
 * <a href="https://developer.veevavault.com/docs/#response-headers">response headers</a>
 * will probably be the long term solution and could make this custom client redundant. Currently, if the API
 * version/endpoint returns a response including header 'X-VaultAPI-Status' with value 'SUCCESS', the response content
 * will not be reset and the request will not be retried. This was verified using v23.2 (current GA version).
 */
public class RetryableClient extends OpenAPIRestClient {

    private static final Logger LOG = LogUtil.getLogger(RetryableClient.class);
    private static final int FIRST = 1;
    private static final String AUTHORIZATION = "Authorization";

    private final VeevaOperationConnection _connection;
    private final RetryStrategy _retryStrategy;

    public RetryableClient(VeevaOperationConnection connection, RetryStrategy retryStrategy) {
        super(connection);
        _connection = connection;
        _retryStrategy = retryStrategy;
    }

    private static ResettableHttpResult getResult(Iterable<HttpResult> results) {
        HttpResult result = CollectionUtil.getFirst(results);
        if (result instanceof ResettableHttpResult) {
            return (ResettableHttpResult) result;
        }

        throw new ConnectorException("Expected a ResettableHttpResult");
    }

    /**
     * Builds and executes incoming {@link RequestBuilder} request. It retries once provided the session is invalid,
     * allowing the connector to clear cache, get a new session, and update the request. If the request should be
     * retried, the response is closed considering its content will not be used.
     *
     * @param request builder for HTTP requests
     * @return Iterable List of HTTP Results
     * @throws IOException if an error occurs executing the request
     */
    @Override
    public Iterable<HttpResult> execute(RequestBuilder request) throws IOException {
        Iterable<HttpResult> httpResults;
        CloseableHttpResponse response;

        boolean shouldRetry = false;
        int retryNumber = FIRST;
        do {
            if (retryNumber > FIRST) {
                // we are clearing the cache to force retrieving a new session ID from the service
                _connection.clearCache();
            }
            request.setHeader(AUTHORIZATION, _connection.getSessionIdFromCache());

            LOG.log(Level.FINE, LogUtils.requestMethodAndUriMessage(request));
            httpResults = super.execute(request);
            response = getResult(httpResults).getResponse();

            try {
                shouldRetry = _retryStrategy.shouldRetry(retryNumber++, response);
            } catch (RuntimeException e) {
                IOUtil.closeQuietly(response);
                throw new ConnectorException("Error executing request: ", e.getMessage(), e);
            } finally {
                if (shouldRetry) {
                    IOUtil.closeQuietly(response);
                }
            }
        } while (shouldRetry);

        return httpResults;
    }

    /**
     * This method is invoked by {@link super#execute(RequestBuilder)}. It is overridden to make the response content
     * stream resettable.
     *
     * @param response HTTP response from the endpoint.
     * @return Single ResettableHttpResult with a content stream that can be reset
     */
    @Override
    protected Iterable<HttpResult> getHttpResults(CloseableHttpResponse response) {
        Iterable<HttpResult> httpResults = super.getHttpResults(response);

        HttpResult result = CollectionUtil.getFirst(httpResults);

        try {
            return Collections.singletonList(new ResettableHttpResult(result, response));
        } catch (IOException e) {
            throw new ConnectorException(e);
        }
    }

    /**
     * Connector result wrapper holding a content stream that can be reset. It exposes the response in
     * {@link #getResponse()} to be used in this class only.
     */
    private static class ResettableHttpResult extends HttpResult {

        private final HttpResult _wrapped;

        public ResettableHttpResult(HttpResult wrapped, CloseableHttpResponse response) throws IOException {
            super(VeevaResponseUtil.getWithResettableContentStream(response));
            _wrapped = wrapped;
        }

        @Override
        public Iterable<? extends Payload> getPayloads() throws IOException {
            return _wrapped.getPayloads();
        }

        private CloseableHttpResponse getResponse() {
            return getSource();
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                IOUtil.closeQuietly(_wrapped);
            }
        }
    }
}