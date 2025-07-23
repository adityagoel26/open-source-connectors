// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.operation.retry;

import com.boomi.util.retry.RetryStrategy;

import org.restlet.data.Status;

/**
 * Custom {@link RetryStrategy} to trigger a single retry when the Status Code in the Response is 401 - Unauthorized.
 * This strategy can be used to force the retrieval of a freshly new access token in case the original one has expired.
 */
public class GoogleBqRetryStrategy extends RetryStrategy {

    private static final int MAX_RETRIES = 1;
    private final RetryStrategy _strategy;

    public GoogleBqRetryStrategy(RetryStrategy strategy) {
        _strategy = strategy;
    }

    @Override
    public boolean shouldRetry(int retryNumber, Object status) {
        boolean retryUnauthorized = Status.CLIENT_ERROR_UNAUTHORIZED.equals(status) && retryNumber <= MAX_RETRIES;
        return retryUnauthorized || (_strategy != null && _strategy.shouldRetry(retryNumber, status));
    }

    @Override
    public void backoff(int retryNumber) {
        if (_strategy != null) {
            _strategy.backoff(retryNumber);
        }
    }
}
