// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.retry;

import com.boomi.util.retry.DurationRetry;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A retry strategy implementation that handles retryable HTTP responses such as 429, 502, 503, and 504.
 * for more details please refer https://developers.hubspot.com/docs/reference/api/other-resources/error-handling .
 * This class extends DurationRetry to provide retry behavior specific to HubSpot API requirements.
 *
 * <p>The strategy will attempt to retry requests that receive retryable HTTP responses up to a specified
 * maximum number of times, with configurable delays between attempts. The retry attempts are also
 * bounded by a maximum total duration.</p>
 */
public class HubspotcrmRetryStrategy extends DurationRetry {

    // Custom HTTP Status Code for Too Many Requests (429)
    private static final int SC_TOO_MANY_REQUESTS = 429;

    /**
     * A set of HTTP status codes considered retryable.
     * <ul>
     *     <li>{@code SC_TOO_MANY_REQUESTS}: Too Many Requests (429)</li>
     *     <li>{@code HttpStatus.SC_BAD_GATEWAY}: Bad Gateway (502)</li>
     *     <li>{@code HttpStatus.SC_SERVICE_UNAVAILABLE}: Service Unavailable (503)</li>
     *     <li>{@code HttpStatus.SC_GATEWAY_TIMEOUT}: Gateway Timeout (504)</li>
     * </ul>
     */
    private static final Set<Integer> RETRYABLE_STATUS_CODES;

    static {
        // Initialize the set of retryable status codes using HTTP status constants
        Set<Integer> codes = new HashSet<>();
        codes.add(SC_TOO_MANY_REQUESTS);                  // 429
        codes.add(org.apache.http.HttpStatus.SC_BAD_GATEWAY);            // 502
        codes.add(org.apache.http.HttpStatus.SC_SERVICE_UNAVAILABLE);    // 503
        codes.add(org.apache.http.HttpStatus.SC_GATEWAY_TIMEOUT);        // 504
        RETRYABLE_STATUS_CODES = Collections.unmodifiableSet(codes);
    }

    /**
     * Constructs a HubspotcrmRetryStrategy with custom parameters.
     *
     * @param maxRetries       Maximum number of retry attempts.
     * @param durationInMillis Maximum duration for retrying in milliseconds.
     * @param delays           Delay values in seconds for each retry phase.
     */
    public HubspotcrmRetryStrategy(int maxRetries, long durationInMillis, int... delays) {
        super(maxRetries, durationInMillis, delays);
    }

    /**
     * Determines whether the caller should retry based on the HTTP response status code.
     * Retries are initiated only for specific retryable status codes and within the maximum retry limit.
     *
     * @param retryNumber The current retry number. The first retry attempt is retry number 1.
     * @param startTime   The start time of the retry logic in milliseconds.
     * @param status      The HTTP response object.
     * @return {@code true} if the caller should retry, {@code false} otherwise.
     */
    @Override
    public boolean shouldRetry(int retryNumber, long startTime, Object status) {
        CloseableHttpResponse response = (CloseableHttpResponse) status;
        int statusCode = 0;

        // Safely extract the HTTP status code from the response
        if (response != null && response.getStatusLine() != null) {
            statusCode = response.getStatusLine().getStatusCode();
        }

        // Check if the status code is retryable and delegate further checks to the parent class
        return isRetryableStatus(statusCode) && super.shouldRetry(retryNumber, startTime, status);
    }

    /**
     * Determines if the given HTTP status code is considered retryable.
     *
     * @param statusCode The HTTP status code to check.
     * @return {@code true} if the status code is retryable, {@code false} otherwise.
     */
    private static boolean isRetryableStatus(int statusCode) {
        return RETRYABLE_STATUS_CODES.contains(statusCode);
    }
}
