// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.retry;

import com.boomi.util.retry.PhasedRetry;

import static org.restlet.data.Status.SERVER_ERROR_SERVICE_UNAVAILABLE;

/**
 * A phase retry strategy when the service response with a 503 server error.
 *
 * @author Magali Kain <magali.kain@boomi.com>.
 */
public class GoogleBqPhasedRetry extends PhasedRetry {

    @Override
    protected boolean shouldRetryImpl(int retryNumber, Object status) {
        return SERVER_ERROR_SERVICE_UNAVAILABLE.equals(status) && super.shouldRetryImpl(retryNumber, status);
    }
}
