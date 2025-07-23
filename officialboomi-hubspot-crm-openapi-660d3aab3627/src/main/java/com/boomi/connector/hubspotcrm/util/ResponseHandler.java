// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.util.ResponseUtil;
import com.boomi.util.StringUtil;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;

/**
 * Utility class for handling HTTP responses in Boomi connectors.
 * This class provides methods to process HTTP responses and extract content safely.
 */
public class ResponseHandler {

    /**
     * Private constructor to prevent instantiation of utility class.
     *
     * @throws UnsupportedOperationException always, as this utility class should not be instantiated
     */
    private ResponseHandler() {
        throw new UnsupportedOperationException("Utility class should not be instantiated");
    }

    /**
     * Handles an HTTP response by processing its status code and content, then updates the operation response
     * accordingly.
     * If the status code matches the expected status, marks the operation as successful.
     * Otherwise, marks it as failed.
     *
     * @param operationResponse the operation response to be updated with the result
     * @param data              the tracked data associated with the operation
     * @param response          the HTTP response to be processed
     * @param expectedStatus    the expected HTTP status code for a successful operation
     * @throws IOException          if there is an error reading the response content or if the response is invalid
     * @throws NullPointerException if operationResponse, data, or response is null
     */
    public static void handleResponse(OperationResponse operationResponse, TrackedData data,
            CloseableHttpResponse response, int expectedStatus, String operation) throws IOException {

        int statusCode = response.getStatusLine().getStatusCode();
        String reasonPhrase = response.getStatusLine().getReasonPhrase();
        Payload payload;
        try (InputStream inputStream = getResponseContent(response)) {
            payload = ResponseUtil.toPayload(inputStream);
        }

        if (expectedStatus == statusCode) {
            operationResponse.addResult(data, OperationStatus.SUCCESS, StringUtil.toString(statusCode), reasonPhrase,
                    payload);
        } else {
            logErrorMsg(statusCode, payload, data, operation);
            operationResponse.addResult(data, OperationStatus.APPLICATION_ERROR, StringUtil.toString(statusCode),
                    reasonPhrase, payload);
        }
    }

    /**
     * Safely extracts the content input stream from an HTTP response.
     * Performs null checks on the response entity and content stream to prevent null pointer exceptions.
     *
     * @param response the HTTP response from which to extract content
     * @return the input stream containing the response content
     * @throws IOException          if the response entity is null, the content stream is null, or there is an error
     *                              accessing the content
     * @throws NullPointerException if response is null
     */
    public static InputStream getResponseContent(CloseableHttpResponse response) throws IOException {

        HttpEntity entity = response.getEntity();
        if (entity == null) {
            throw new IOException("Response entity is null");
        }

        InputStream content = entity.getContent();

        if (content == null) {
            throw new IOException("Response content stream is null");
        }
        return content;
    }

    /**
     * Handles an IOException that occurred during an operation.
     * Logs the error message and adds a failure to the operation response.
     *
     * @param operationResponse the operation response to be updated with the failure
     * @param data              the tracked data associated with the operation
     * @param e                 the IOException that occurred
     * @param operation         the type of operation that failed
     * @throws NullPointerException if operationResponse, data, or e is null
     */
    public static void handleError(OperationResponse operationResponse, TrackedData data, IOException e,
            String operation) {
        String msg = String.format("IOException occurred while executing %s operation: %s", operation, e.getMessage());
        data.getLogger().log(Level.SEVERE, msg);
        ResponseUtil.addExceptionFailure(operationResponse, data, e);
    }

    /**
     * Logs an error message with the status code and response content.
     *
     * @param statusCode the HTTP status code of the failed response
     * @param payload    the payload containing the response content
     * @param data       the tracked data associated with the operation
     * @param operation  the type of operation that failed
     * @throws NullPointerException if data is null
     */
    public static void logErrorMsg(int statusCode, Payload payload, TrackedData data, String operation) {
        String msg = String.format("Failed to execute %s operation. Status code: %d, Response: %s", operation,
                statusCode, payload.toString());
        data.getLogger().log(Level.SEVERE, msg);
    }
}
