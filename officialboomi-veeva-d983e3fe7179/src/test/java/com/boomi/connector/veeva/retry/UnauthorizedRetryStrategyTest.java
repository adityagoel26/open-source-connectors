// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.veeva.retry;

import com.boomi.util.IOUtil;

import org.apache.http.Header;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.message.BasicHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UnauthorizedRetryStrategyTest {

    private static final String JSON_CHARSET_UTF_8 = "application/json;charset=UTF-8";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream;charset=UTF-8";

    private static final String SUCCESS_RESPONSE = "{\n" + "\t\"responseStatus\": \"SUCCESS\"\n" + "}";
    private static final String SUCCESS_RESPONSE_WITH_ADDITIONAL_FIELDS =
            "{\n" + "\t\"dummyField\": \"DUMMY_VALUE\",\n" + "\t\"responseStatus\": \"SUCCESS\",\n"
            + "\t\"anotherDummyField\": \"DUMMY_VALUE\"\n" + "}";
    private static final String INVALID_SESSION_RESPONSE =
            "{\n" + "\t\"responseStatus\": \"FAILURE\",\n" + "\t\"errors\": [{\n"
            + "\t\t\"type\": \"INVALID_SESSION_ID\",\n" + "\t\t\"message\": \"Invalid or expired session ID.\"\n"
            + "\t}]\n" + "}";

    private static final String INVALID_SESSION_WITH_ADDITIONAL_FIELDS =
            "{\n" + "\t\"dummyField\": \"DUMMY_VALUE\",\n" + "\t\"responseStatus\": \"FAILURE\",\n"
            + "\t\"errors\": [{\n" + "\t\t\"type\": \"INVALID_SESSION_ID\",\n"
            + "\t\t\"message\": \"Invalid or expired session ID.\"\n" + "\t}]\n" + "}";

    private static final String INVALID_PARAM_REQUIRED_RESPONSE =
            "{\n" + "\t\"responseStatus\": \"FAILURE\",\n" + "\t\"errors\": [{\n"
            + "\t\t\"type\": \"PARAMETER_REQUIRED\",\n" + "\t\t\"message\": \"Missing required parameter [size]\"\n"
            + "\t}]\n" + "}";

    private static final String INVALID_SESSION_WITH_ERRORS_FIRST =
            "{\n" + "\t\"errors\": [{\n" + "\t\t\"type\": \"INVALID_SESSION_ID\",\n"
            + "\t\t\"message\": \"Invalid or expired session ID.\"\n" + "\t}],\n"
            + "\t\"dummyField\": \"DUMMY_VALUE\",\n" + "\t\"responseStatus\": \"FAILURE\",\n"
            + "\t\"anotherDummyField\": \"DUMMY_VALUE\",\n" + "}";

    private static final String INVALID_SESSION_WITH_MULTIPLE_ARRAYS =
            "{\n" + "\t\"other\": [{\n" + "\t\t\"type\": \"dummy\",\n" + "\t\t\"message\": \"dummy\"\n" + "\t}],\n"
            + "\t\"errors\": [{\n" + "\t\t\"type\": \"INVALID_SESSION_ID\",\n"
            + "\t\t\"message\": \"Invalid or expired session ID.\"\n" + "\t}],\n"
            + "\t\"dummyField\": \"DUMMY_VALUE\",\n" + "\t\"responseStatus\": \"FAILURE\",\n"
            + "\t\"anotherDummyField\": \"DUMMY_VALUE\",\n" + "}";

    private static void shouldRetryTrue(String content) throws IOException {
        shouldRetry(content, JSON_CHARSET_UTF_8, 1, Assertions::assertTrue);
    }

    private static void shouldRetryFalse(String content, String contentTypeValue) throws IOException {
        shouldRetry(content, contentTypeValue, 1, Assertions::assertFalse);
    }

    private static void shouldRetry(String content, String contentTypeValue, int retryNumber, Consumer<Boolean> verify)
            throws IOException {
        shouldRetry(content, contentTypeValue, retryNumber, verify, "FAILURE");
    }

    private static void shouldRetry(String content, String contentTypeValue, int retryNumber,
            Consumer<Boolean> assertion, String vaultApiStatusValue) throws IOException {
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        CloseableHttpResponse response = mock(CloseableHttpResponse.class, RETURNS_DEEP_STUBS);
        Header contentType = new BasicHeader("Content-type", contentTypeValue);
        Header vaultApiStatus = new BasicHeader("X-VaultAPI-Status", vaultApiStatusValue);

        when(response.getStatusLine().getStatusCode()).thenReturn(200);
        when(response.getEntity().getContentLength()).thenReturn(1L);
        when(response.getEntity().getContent()).thenReturn(inputStream);
        when(response.getFirstHeader("Content-Type")).thenReturn(contentType);
        when(response.getFirstHeader("X-VaultAPI-Status")).thenReturn(vaultApiStatus);

        UnauthorizedRetryStrategy retryStrategy = new UnauthorizedRetryStrategy();
        assertion.accept(retryStrategy.shouldRetry(retryNumber, response));

        IOUtil.closeQuietly(inputStream);
    }

    @Test
    public void shouldRetryInvalidSession() throws IOException {
        shouldRetryTrue(INVALID_SESSION_RESPONSE);
    }

    @Test
    public void shouldRetryWhenThereAreMoreFields() throws IOException {
        shouldRetryTrue(INVALID_SESSION_WITH_ADDITIONAL_FIELDS);
    }

    @Test
    public void shouldRetryInvalidSessionWhenErrorsFirst() throws IOException {
        shouldRetryTrue(INVALID_SESSION_WITH_ERRORS_FIRST);
    }

    @Test
    public void shouldRetryInvalidSessionWithMultipleArrays() throws IOException {
        shouldRetryTrue(INVALID_SESSION_WITH_MULTIPLE_ARRAYS);
    }

    @Test
    public void shouldNotRetryInvalidSessionMoreThanOnce() throws IOException {
        shouldRetry(INVALID_SESSION_RESPONSE, JSON_CHARSET_UTF_8, 2, Assertions::assertFalse);
    }

    @Test
    public void shouldNotRetryOtherFailures() throws IOException {
        shouldRetryFalse(INVALID_PARAM_REQUIRED_RESPONSE, JSON_CHARSET_UTF_8);
    }

    @Test
    public void shouldNotRetrySuccessfulResponse() throws IOException {
        shouldRetryFalse(SUCCESS_RESPONSE_WITH_ADDITIONAL_FIELDS, JSON_CHARSET_UTF_8);
    }

    @Test
    public void shouldNotRetryOctetStreamResponse() throws IOException {
        shouldRetryFalse(SUCCESS_RESPONSE, APPLICATION_OCTET_STREAM);
    }

    @Test
    public void shouldNotRetryResponseWithVaultApiSuccessfulHeader() throws IOException {
        shouldRetry(INVALID_SESSION_RESPONSE, JSON_CHARSET_UTF_8, 1, Assertions::assertFalse, "SUCCESS");
    }
}
