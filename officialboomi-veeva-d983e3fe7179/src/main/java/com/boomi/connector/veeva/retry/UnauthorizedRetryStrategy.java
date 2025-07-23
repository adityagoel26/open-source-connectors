// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.retry;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.boomi.util.retry.RetryStrategy;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.IOException;
import java.io.InputStream;

/**
 * Strategy that retries once provided the response is unauthorized due to an invalid/expired session id. This might
 * require to parse the response to check whether it does not include a status field {@link #SUCCESS} but do include an
 * array of errors including an error element of type {@link #INVALID_SESSION_ID}.
 */
public class UnauthorizedRetryStrategy extends RetryStrategy {

    private static final String RESPONSE_STATUS = "responseStatus";
    private static final String SUCCESS = "SUCCESS";
    private static final String ERRORS = "errors";
    private static final String TYPE = "type";
    private static final String INVALID_SESSION_ID = "INVALID_SESSION_ID";
    private static final int FIRST = 1;

    /**
     * Determines whether the request should be retried based on the response content. The content stream is reset, so
     * it can be reused.
     *
     * @param response an HTTP response
     * @return {@code true} if the response contains an invalid session error, {@code false} if not.
     */
    private static boolean shouldRetryPerContent(CloseableHttpResponse response) {
        boolean shouldRetry;

        JsonParser parser = null;
        try {
            InputStream contentStream = response.getEntity().getContent();
            parser = JSONUtil.getDefaultJsonFactory().createParser(contentStream);

            shouldRetry = isInvalidSession(parser);

            contentStream.reset();
        } catch (Exception e) {
            throw new ConnectorException(e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(parser);
        }

        return shouldRetry;
    }

    private static boolean isInvalidSession(JsonParser parser) throws IOException {
        while (shouldContinue(parser.nextToken())) {
            if (JsonToken.FIELD_NAME != parser.getCurrentToken()) {
                // keep reading until getting the next field
                continue;
            }

            String fieldName = parser.getCurrentName();
            if (RESPONSE_STATUS.equals(fieldName) && isSuccessStatus(parser)) {
                // the response is successful, stop reading the json
                return false;
            }

            if (ERRORS.equals(fieldName)) {
                // errors array found, return whether it contains an element with type invalid session
                return arrayHasInvalidSessionError(parser);
            }
        }

        // no error found
        return false;
    }

    private static boolean shouldContinue(JsonToken jsonToken) {
        return (jsonToken != null);
    }

    private static boolean isSuccessStatus(JsonParser parser) throws IOException {
        parser.nextToken();
        return SUCCESS.equals(parser.getValueAsString());
    }

    private static boolean arrayHasInvalidSessionError(JsonParser parser) throws IOException {
        if (JsonToken.START_ARRAY != parser.nextToken()) {
            return false;
        }

        while (JsonToken.END_ARRAY != parser.nextToken()) {
            // traverse the errors
            if (elementHasInvalidSessionError(parser)) {
                // error with invalid session type found
                return true;
            }
        }

        // error with invalid session type not found
        return false;
    }

    private static boolean elementHasInvalidSessionError(JsonParser parser) throws IOException {
        if (JsonToken.FIELD_NAME != parser.getCurrentToken()) {
            return false;
        }

        // return whether the field name is type, and the field value is invalid session
        return TYPE.equals(parser.getCurrentName()) && isInvalidSessionIdValue(parser);
    }

    private static boolean isInvalidSessionIdValue(JsonParser parser) throws IOException {
        parser.nextToken();
        return INVALID_SESSION_ID.equals(parser.getValueAsString());
    }

    /**
     * Determines whether the caller should initiate a retry, based on the retryNumber and the status (in this
     * implementation the service http response).
     * <p>
     * If the response does not include a header stating that it was successful, it has content of type is json, and the
     * content includes an error stating that the session id is invalid/expired, the status indicates to retry.
     *
     * @param retryNumber The current retry number. First retry attempt to call shouldRetry is retry number 1.
     * @param status      the response status
     * @return {@code true} if the caller should retry, {@code false} if not.
     */
    @Override
    public boolean shouldRetry(int retryNumber, Object status) {
        if (retryNumber > FIRST) {
            return false;
        }

        CloseableHttpResponse response = (CloseableHttpResponse) status;

        return (!VeevaResponseUtil.isVaultApiStatusSuccess(response)) && VeevaResponseUtil.isContentTypeJson(response)
               && VeevaResponseUtil.hasContent(response) && shouldRetryPerContent(response);
    }

    @Override
    public void backoff(int i) {
        // do nothing
    }
}