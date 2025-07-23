// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.util;

import com.boomi.common.apache.http.entity.RepeatableInputStreamEntity;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.veeva.VeevaBaseConnection;
import com.boomi.connector.veeva.retry.UnauthorizedRetryStrategy;
import com.boomi.connector.veeva.retry.VeevaResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class containing common execution functionality.
 */
public class ExecutionUtils {

    private static final String AUTHORIZATION = "Authorization";
    private static final int UNKNOWN_LENGTH = -1;
    private static final int FIRST = 1;
    private static final Logger LOG = LogUtil.getLogger(ExecutionUtils.class);

    private ExecutionUtils() {
    }

    /**
     * Builds and executes an HTTP request. It retries once provided the response payload indicates the session id
     * provided is invalid, allowing the connector to clear cache and get a new session.
     *
     * @param httpMethod  the http method
     * @param path        the request path
     * @param client      the client to execute the request
     * @param inputStream the stream for content requests
     * @param filterData  for logging purposes and to retrieve the heeaders
     * @return an HTTP response
     * @throws IOException if an error occurs executing the request
     */
    public static CloseableHttpResponse execute(String httpMethod, String path, CloseableHttpClient client,
            InputStream inputStream, VeevaBaseConnection connection, FilterData filterData) throws IOException {
        CloseableHttpResponse response;
        UnauthorizedRetryStrategy retryStrategy = new UnauthorizedRetryStrategy();

        // url returned by connection ends with a trailing slash and path string starts with a slash as well
        String url = URLUtil.trimTrailingSeparators(connection.getUrl()) + path;

        HttpRequestBase httpRequest = getHttpRequest(httpMethod, url, inputStream);
        ClientIDUtils.setClientID(httpRequest, connection.getContext());
        setCustomHeaders(httpRequest, connection.getCustomHeaders(filterData));

        boolean shouldRetry = false;
        int retryNumber = FIRST;
        do {
            if (retryNumber > FIRST) {
                // we are clearing the cache to force retrieving a new session ID from the service
                connection.clearCache();
            }
            httpRequest.setHeader(AUTHORIZATION, connection.getSessionIdFromCache());
            logRequest((filterData != null) ? filterData.getLogger() : null, httpRequest);
            response = VeevaResponseUtil.getWithResettableContentStream(client.execute(httpRequest));

            try {
                shouldRetry = retryStrategy.shouldRetry(retryNumber++, response);
            } catch (RuntimeException e) {
                IOUtil.closeQuietly(response);
                throw new ConnectorException("Error executing request: ", e.getMessage(), e);
            } finally {
                if (shouldRetry) {
                    IOUtil.closeQuietly(response);
                }
            }
        } while (shouldRetry);

        return response;
    }

    public static CloseableHttpResponse execute(String httpMethod, String path, CloseableHttpClient client,
            InputStream inputStream, VeevaBaseConnection connection) throws IOException {
        return execute(httpMethod, path, client, inputStream, connection, null);
    }

    /**
     * Logs the request method and URL on both a Container logger and a provided logger if it's not null
     *
     * @param logger  the provided logger
     * @param request the request to log
     */
    private static void logRequest(Logger logger, HttpUriRequest request) {
        Supplier<String> msgSupplier = LogUtils.requestMethodAndUriMessage(request);
        if (logger != null) {
            // process executions
            logger.log(Level.INFO, msgSupplier);
        } else {
            // browser and test connection
            LOG.log(Level.FINE, msgSupplier);
        }
    }

    private static HttpRequestBase getHttpRequest(String httpMethod, String url, InputStream inputStream) {
        switch (httpMethod) {
            case "DELETE":
                return new HttpDelete(url);
            case "GET":
                return new HttpGet(url);
            case "POST":
                return getContentRequest(new HttpPost(url), inputStream);
            case "PATCH":
                return getContentRequest(new HttpPatch(url), inputStream);
            case "PUT":
                return getContentRequest(new HttpPut(url), inputStream);
            default:
                throw new IllegalArgumentException("HTTP Method not supported: " + httpMethod);
        }
    }

    private static HttpRequestBase getContentRequest(HttpEntityEnclosingRequestBase request, InputStream inputStream) {
        HttpEntity requestEntity = new RepeatableInputStreamEntity(inputStream, UNKNOWN_LENGTH,
                ContentType.APPLICATION_FORM_URLENCODED);
        request.setEntity(requestEntity);
        return request;
    }

     static void setCustomHeaders(HttpRequestBase request, Map<String, String> customHeaders) {
        customHeaders.entrySet().stream().filter(header -> StringUtil.isNotEmpty(header.getValue())).forEach(
                header -> request.setHeader(header.getKey(), header.getValue()));
    }
}
