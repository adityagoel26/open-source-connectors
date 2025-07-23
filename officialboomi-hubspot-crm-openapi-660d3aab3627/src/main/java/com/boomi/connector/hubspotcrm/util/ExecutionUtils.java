// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.common.apache.http.entity.RepeatableInputStreamEntity;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.hubspotcrm.browser.HubspotcrmOperationType;
import com.boomi.connector.hubspotcrm.retry.HubspotcrmRetryStrategy;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class containing common execution functionality.
 */
public class ExecutionUtils {

    private static final int UNKNOWN_LENGTH = -1;
    public static final String COMMA_SEPARATOR = ",";
    private static final String SLASH = "/";
    private static final int OBJECT_TYPE_INDEX = 2;
    private static final int FIRST = 1;
    // ObjectMapper instance
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    // Default retry configuration values
    private static final int DEFAULT_MAX_RETRIES = 3;
    private static final int[] RETRY_DELAYS_S = { 5, 10, 20 };
    private static final long DEFAULT_MAX_WAIT_TIME_MS = 90_000L;

    private ExecutionUtils() {
    }

    /**
     * Returns the singleton instance of ObjectMapper.
     *
     * @return The configured ObjectMapper instance
     */
    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    /**
     * Executes an HTTP request to the HubSpot CRM API with retry logic.
     *
     * <p>This method implements a retry mechanism for handling retryable HTTP responses
     * (e.g., 429 Too Many Requests, 502 Bad Gateway). Retries are limited
     * by the default maximum number of retries, wait time, and exponential backoff
     * defined in the retry strategy.</p>
     *
     * @param httpMethod   The HTTP method to use (GET, POST, PUT, DELETE, etc.)
     * @param path         The API endpoint path to append to the base URL
     * @param client       The HTTP client instance to use
     * @param inputStream  The request body as an InputStream (optional for non-POST/PUT requests)
     * @param apiUrl     The HubSpot api URL
     * @param headers      List of HTTP headers as key-value pairs
     * @return A CloseableHttpResponse containing the server's response
     * @throws IOException        If an I/O error occurs during request execution
     * @throws ConnectorException If an error occurs during retry handling or request execution
     */
    public static CloseableHttpResponse execute(String httpMethod, String path, CloseableHttpClient client,
            InputStream inputStream, URL apiUrl, List<Map.Entry<String, String>> headers, Logger logger)
            throws ConnectorException, IOException {
        String url = String.format("%s%s", apiUrl, path);
        HubspotcrmRetryStrategy retryStrategy = new HubspotcrmRetryStrategy(DEFAULT_MAX_RETRIES,
                DEFAULT_MAX_WAIT_TIME_MS, RETRY_DELAYS_S);

        HttpRequestBase httpRequest = getHttpRequest(httpMethod, url, inputStream);
        headers.forEach(header -> httpRequest.addHeader(header.getKey(), header.getValue()));

        return executeWithRetries(httpRequest, client, retryStrategy, path, logger);
    }

    /**
     * Executes an HTTP request with retries for retryable HTTP responses.
     *
     * <p>Retryable responses include HTTP 429 (Too Many Requests),
     * 502 (Bad Gateway), 503 (Service Unavailable), and 504 (Gateway Timeout).</p>
     *
     * @param httpRequest   The HTTP request to execute
     * @param client        The HTTP client instance
     * @param retryStrategy The retry strategy to use for handling retryable responses
     * @param path          The API endpoint path for logging purposes
     * @return A CloseableHttpResponse containing the server's response
     * @throws IOException If an I/O error occurs during request execution
     */
    private static CloseableHttpResponse executeWithRetries(HttpRequestBase httpRequest, CloseableHttpClient client,
            HubspotcrmRetryStrategy retryStrategy, String path, Logger logger) throws IOException {
        CloseableHttpResponse response = null;
        boolean shouldRetry = false;
        int retryNumber = FIRST;
        long startTime = System.currentTimeMillis();

        do {
            try {
                response = HubspotcrmResponseUtil.getWithResettableContentStream(client.execute(httpRequest));
                shouldRetry = retryStrategy.shouldRetry(retryNumber, startTime, response);
            } catch (RuntimeException e) {
                IOUtil.closeQuietly(response);
                throw new ConnectorException("Error executing request: " + e.getMessage(), e);
            } finally {
                if (shouldRetry) {
                    int finalRetryNumber = retryNumber;
                    if (logger != null) {
                        logger.log(Level.FINEST, () -> String.format("Retrying API request for path '%s'."
                                        + "Retry attempt: %d, Backoff duration: %d seconds", path, finalRetryNumber,
                                RETRY_DELAYS_S[finalRetryNumber - 1]));
                    }
                    IOUtil.closeQuietly(response);
                    retryStrategy.backoff(retryNumber++);
                }
            }
        } while (shouldRetry);

        return response;
    }

    /**
     * Creates an HTTP request based on the provided HTTP method, URL, and input stream.
     *
     * @param httpMethod  The HTTP method for the request (e.g., GET, POST, PUT, DELETE, PATCH).
     * @param url         The URL for the request.
     * @param inputStream The input stream representing the request body (for POST, PUT, and PATCH requests).
     * @return The corresponding HTTP request base object.
     * @throws IllegalArgumentException If the provided HTTP method is not supported.
     */
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

    /**
     * Creates an HTTP request with the provided input stream as the request body.
     *
     * @param request     The HTTP request base object to be modified.
     * @param inputStream The input stream representing the request body.
     * @return The modified HTTP request base object with the input stream set as the request body.
     */
    private static HttpRequestBase getContentRequest(HttpEntityEnclosingRequestBase request, InputStream inputStream) {
        HttpEntity requestEntity = new RepeatableInputStreamEntity(inputStream, UNKNOWN_LENGTH,
                ContentType.APPLICATION_FORM_URLENCODED);
        request.setEntity(requestEntity);
        return request;
    }

    /**
     * Encodes a value for safe inclusion in a URL.
     *
     * @param value The value to encode.
     * @return The URL-encoded string.
     * @throws UnsupportedEncodingException if the character encoding is not supported.
     */
    public static String urlEncode(String value) throws UnsupportedEncodingException {
        return URLEncoder.encode(value, String.valueOf(StandardCharsets.UTF_8));
    }

    /**
     * Extracts the base path from the object type ID based on the operation type.
     *
     * @param operationType The type of operation being performed (e.g., UPDATE, QUERY)
     * @param objectTypeId  The full object type ID path that needs to be parsed
     * @return The base path extracted from the object type ID. For UPDATE/QUERY operations,
     * returns the path component at index specified by OBJECT_TYPE_INDEX
     * from the end. For all other operations, returns the last component of the path.
     */
    public static String getRootNode(HubspotcrmOperationType operationType, String objectTypeId) {
        String[] parts = objectTypeId.split(SLASH);
        if (operationType == HubspotcrmOperationType.UPDATE || operationType == HubspotcrmOperationType.QUERY) {
            return parts[parts.length - OBJECT_TYPE_INDEX];
        } else {
            return parts[parts.length - 1];
        }
    }

    /**
     * Adds a successful result to the operation response.
     *
     * @param operationResponse The response object to populate
     * @param filterData        The filter data for the operation
     * @param statusCode        The status code for the result
     * @param status            The status message for the result
     * @param payload           The payload containing the result data
     */
    public static void addSuccessResult(OperationResponse operationResponse, FilterData filterData, String statusCode,
            String status, Payload payload) {
        operationResponse.addPartialResult(filterData, OperationStatus.SUCCESS, statusCode, status, payload);
    }

    /**
     * Extracts the property name from a path-based field identifier.
     *
     * @param property The full property path
     * @return The extracted property name
     * @throws IllegalArgumentException if the property path is invalid
     */
    public static String extractPropertyName(String property) {
        if (StringUtil.isBlank(property)) {
            throw new IllegalArgumentException(HubspotcrmConstant.PROPERTY_EMPTY_MSG);
        }
        String[] parts = property.split(HubspotcrmConstant.SLASH);
        if (parts.length < HubspotcrmConstant.EXPECTED_AMOUNT_OF_ELEMENTS_IN_OBJECT_TYPE) {
            throw new IllegalArgumentException(HubspotcrmConstant.PROPERTY_FORMAT_MSG);
        }
        return parts[1];
    }
}
