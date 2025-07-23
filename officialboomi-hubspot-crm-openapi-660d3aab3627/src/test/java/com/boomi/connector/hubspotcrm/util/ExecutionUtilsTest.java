// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.hubspotcrm.HubspotcrmConnection;
import com.boomi.connector.hubspotcrm.retry.HubspotcrmRetryStrategy;
import io.swagger.models.HttpMethod;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collections;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * Test class for validating the behavior of {@link ExecutionUtils} and {@link HubspotcrmRetryStrategy}.
 *
 * <p>Provides comprehensive test cases for:
 * <ul>
 *   <li>Retry logic for HTTP responses (e.g., 429, 502, 503, 504).</li>
 *   <li>Exception handling during execution.</li>
 *   <li>Stopping retries after a successful response.</li>
 *   <li>Handling of non-retryable status codes.</li>
 *   <li>Behavior during interrupted backoff periods.</li>
 *   <li>Logging and backoff invocation during retries.</li>
 * </ul>
 * </p>
 */
class ExecutionUtilsTest {

    private static final int RATE_LIMIT_EXCEED = 429;
    private static final int[] RETRY_DELAYS = {5, 10, 20};
    private static final long CUSTOM_MAX_WAIT_TIME = 15000;
    private static final int CUSTOM_MAX_RETRIES = 3;
    private static final String SAMPLE_API = "https://mockapi.example.com";

    /**
     * Tests retry logic for all retriable HTTP status codes without actual delays.
     *
     * <p>Verifies that retries occur for the following status codes:
     * <ul>
     *   <li>429 Too Many Requests</li>
     *   <li>502 Bad Gateway</li>
     *   <li>503 Service Unavailable</li>
     *   <li>504 Gateway Timeout</li>
     * </ul>
     * Eliminates backoff delays for faster execution during tests.
     * </p>
     *
     * @throws IOException If an I/O error occurs during the test.
     */
    @Test
    void testRetryableStatusCodes() throws IOException {
        int[] retriableStatusCodes = {
                RATE_LIMIT_EXCEED, // 429
                HttpStatus.SC_BAD_GATEWAY,           // 502
                HttpStatus.SC_SERVICE_UNAVAILABLE,   // 503
                HttpStatus.SC_GATEWAY_TIMEOUT        // 504
        };

        // Custom strategy for testing with overridden backoff method
        class TestRetryStrategy extends HubspotcrmRetryStrategy {
            public TestRetryStrategy(int maxRetries, long durationInMillis, int... delays) {
                super(maxRetries, durationInMillis, delays);
            }

            @Override
            public void backoff(int retryNumber) {
                // No-op to avoid actual delay during testing
            }
        }

        for (int statusCode : retriableStatusCodes) {
            CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
            StatusLine statusLine = Mockito.mock(StatusLine.class);

            Mockito.when(response.getStatusLine()).thenReturn(statusLine);
            Mockito.when(statusLine.getStatusCode()).thenReturn(statusCode);

            TestRetryStrategy retryStrategy = new TestRetryStrategy(CUSTOM_MAX_RETRIES,
                    CUSTOM_MAX_WAIT_TIME, RETRY_DELAYS);

            boolean shouldRetry = retryStrategy.shouldRetry(1, System.currentTimeMillis(), response);
            Assertions.assertTrue(shouldRetry, "Should retry on status code: " + statusCode);

            for (int retryNumber = 1; retryNumber <= CUSTOM_MAX_RETRIES; retryNumber++) {
                shouldRetry = retryStrategy.shouldRetry(retryNumber, System.currentTimeMillis(), response);
                Assertions.assertTrue(shouldRetry, String.format("Retry %d should be allowed for status code %d", retryNumber, statusCode));
            }

            shouldRetry = retryStrategy.shouldRetry(CUSTOM_MAX_RETRIES + 1, System.currentTimeMillis(), response);
            Assertions.assertFalse(shouldRetry, "Should not retry after exceeding max retries for status code: " + statusCode);
        }
    }

    /**
     * Tests that retries stop after receiving a successful response (200 OK).
     *
     * <p>Verifies the retry logic transitions from retryable responses (e.g., 502) to stopping retries
     * when a 200 OK response is received.</p>
     *
     * @throws IOException If an I/O error occurs during the test.
     */
    @Test
    void testRetryStopsAfterSuccess() throws IOException {
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);

        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode())
                .thenReturn(HttpStatus.SC_BAD_GATEWAY) // 502 on first call
                .thenReturn(HttpStatus.SC_OK); // 200 on second call

        HubspotcrmRetryStrategy retryStrategy = new HubspotcrmRetryStrategy(CUSTOM_MAX_RETRIES,
                CUSTOM_MAX_WAIT_TIME, RETRY_DELAYS);

        boolean shouldRetry = retryStrategy.shouldRetry(1, System.currentTimeMillis(), response);
        Assertions.assertTrue(shouldRetry, "Should retry on first retriable response (502)");

        retryStrategy.backoff(1);

        shouldRetry = retryStrategy.shouldRetry(2, System.currentTimeMillis(), response);
        Assertions.assertFalse(shouldRetry, "Should not retry after receiving 200 OK");
    }

    /**
     * Tests exception handling during execution.
     *
     * <p>Verifies that the method correctly catches {@link RuntimeException}
     * and wraps it in a {@link ConnectorException}.</p>
     *
     * @throws IOException If an I/O error occurs during the test.
     */
    @Test
    void testExecuteHandlesRuntimeException() throws IOException {
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HubspotcrmConnection connection = Mockito.mock(HubspotcrmConnection.class);
        InputStream inputStream = Mockito.mock(InputStream.class);

        Mockito.when(connection.getUrl()).thenReturn(new URL(SAMPLE_API));

        Mockito.when(client.execute(Mockito.any())).thenThrow(new RuntimeException("Simulated runtime exception"));

        ConnectorException thrown = Assertions.assertThrows(ConnectorException.class, () -> {
            ExecutionUtils.execute(HttpMethod.GET.name(), "/test", client, inputStream, connection.getUrl(),
                    Collections.emptyList(), null);
        });

        Assertions.assertTrue(thrown.getMessage().contains("Error executing request"),
                "ConnectorException should contain the expected error message");
    }

    /**
     * Verifies that logging and backoff behavior occur during retries.
     *
     * <p>Ensures that:
     * <ul>
     *   <li>The backoff method is invoked during retries.</li>
     *   <li>Retry attempts are logged appropriately.</li>
     * </ul>
     * </p>
     *
     * @throws IOException If an I/O error occurs during the test.
     */
    @Test
    void testExecuteLogsAndBackoffDuringRetry() throws IOException {
        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HubspotcrmConnection connection = Mockito.mock(HubspotcrmConnection.class);
        InputStream inputStream = Mockito.mock(InputStream.class);

        Mockito.when(connection.getUrl()).thenReturn(new URL(SAMPLE_API));
        Mockito.when(client.execute(Mockito.any())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode())
                .thenReturn(RATE_LIMIT_EXCEED) // Retryable error
                .thenReturn(HttpStatus.SC_OK); // Success on second attempt

        ExecutionUtils.execute(HttpMethod.GET.name(), "/test", client, inputStream, connection.getUrl(), Collections.emptyList(), null);

        Mockito.verify(client, Mockito.times(2)).execute(Mockito.any());
    }

    /**
     * Verifies the behavior of retry logic when a null response is encountered.
     *
     * <p>Ensures the logic gracefully handles null responses without attempting retries.</p>
     */
    @Test
    void testRetryWithNullResponse() {
        HubspotcrmRetryStrategy retryStrategy = new HubspotcrmRetryStrategy(CUSTOM_MAX_RETRIES,
                CUSTOM_MAX_WAIT_TIME, RETRY_DELAYS);

        boolean shouldRetry = retryStrategy.shouldRetry(1, System.currentTimeMillis(), null);
        Assertions.assertFalse(shouldRetry, "Should not retry when response is null");
    }

    /**
     * Tests the behavior of the retry logic during an interrupted backoff period.
     *
     * <p>Ensures the interrupt flag is properly handled when the thread is interrupted.</p>
     */
    @Test
    void testRetryWithInterruptedBackoff() {
        HubspotcrmRetryStrategy retryStrategy = new HubspotcrmRetryStrategy(CUSTOM_MAX_RETRIES,
                CUSTOM_MAX_WAIT_TIME, RETRY_DELAYS);

        Thread.currentThread().interrupt();
        retryStrategy.backoff(1);

        Assertions.assertTrue(Thread.interrupted(), "Interrupt flag should be set after backoff is interrupted");
    }

    /**
     * Tests retry logic with a logger to validate logging behavior during retries.
     *
     * @throws IOException If an I/O error occurs during the test.
     */
    @Test
    void testRetryLoggingWithLogger() throws IOException {
        Logger mockLogger = Mockito.mock(Logger.class);

        CloseableHttpResponse response = Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = Mockito.mock(StatusLine.class);
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        HubspotcrmConnection connection = Mockito.mock(HubspotcrmConnection.class);
        InputStream inputStream = Mockito.mock(InputStream.class);

        Mockito.when(connection.getUrl()).thenReturn(new URL(SAMPLE_API));
        Mockito.when(client.execute(Mockito.any())).thenReturn(response);
        Mockito.when(response.getStatusLine()).thenReturn(statusLine);
        Mockito.when(statusLine.getStatusCode())
                .thenReturn(RATE_LIMIT_EXCEED) // Retryable error
                .thenReturn(HttpStatus.SC_OK); // Success on second attempt

        ExecutionUtils.execute(HttpMethod.GET.name(), "/test", client, inputStream, connection.getUrl(), Collections.emptyList(), mockLogger);

        // Verify logging and retry behavior
        Mockito.verify(client, Mockito.times(2)).execute(Mockito.any());
        // Verify the logger was called
        Mockito.verify(mockLogger, Mockito.times(1)).log(
                Mockito.eq(Level.FINEST),
                Mockito.<Supplier<String>>argThat(arg -> {
                    String message = arg.get();
                    return message.contains("Retrying API request for path '/test'")
                            && message.contains("Retry attempt: 1")
                            && message.contains("Backoff duration: 5 seconds");
                })
        );
    }

    /**
     * Tests that the ObjectMapper singleton is properly returned.
     */
    @Test
    void testGetObjectMapper() {
        Assertions.assertNotNull(ExecutionUtils.getObjectMapper(), "ObjectMapper instance should not be null");
        Assertions.assertSame(ExecutionUtils.getObjectMapper(), ExecutionUtils.getObjectMapper(),
                "ObjectMapper should always return the same singleton instance");
    }
}