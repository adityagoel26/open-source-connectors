//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.requests;

import com.boomi.util.retry.RetryStrategy;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

/**
 * Custom implementation of {@link RetryStrategy} to work with Apache Http Client and allow retries if
 * a 401 - Unauthorized code is received as it may imply that the access token stored in cache has expired.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class PrismRetryStrategy extends RetryStrategy {

    private final int maxAttempts;

    /**
     * Creates a new {@link PrismRetryStrategy} instance
     *
     * @param maxAttempts
     *         how many retries attempts will be allowed.
     */
    PrismRetryStrategy(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    /**
     * Will return true if a 401 - Unauthorized code was received and the maximum amount of retries haven't
     * been reached.
     *
     * @param retryNumber
     *         indicates the retry number for the invocation.
     * @param httpResponse
     *         must be an instance of {@link CloseableHttpResponse}.
     */
    @Override
    public boolean shouldRetry(int retryNumber, Object httpResponse) {
        if (retryNumber < maxAttempts && httpResponse instanceof CloseableHttpResponse) {
            CloseableHttpResponse response = (CloseableHttpResponse) httpResponse;
            return isUnauthorized(response.getStatusLine());
        }

        return false;
    }

    @Override
    public void backoff(int retryNumber) {
        // Do nothing
    }

    private static boolean isUnauthorized(StatusLine statusLine) {
        return statusLine != null && statusLine.getStatusCode() == HttpStatus.SC_UNAUTHORIZED;
    }
}
