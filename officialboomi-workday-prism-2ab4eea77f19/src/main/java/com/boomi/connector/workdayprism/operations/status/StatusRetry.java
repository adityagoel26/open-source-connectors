//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.workdayprism.operations.status;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.utils.Constants;
import com.boomi.util.LogUtil;
import com.boomi.util.retry.SleepingRetry;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class StatusRetry extends SleepingRetry {
    private static final Logger LOG = LogUtil.getLogger(StatusRetry.class);

    private static final String ERROR_BUCKET_STATUS = "cannot retrieve the status of the bucket.";
    private static final long DEFAULT_RETRY = TimeUnit.SECONDS.toMillis(30L);
    private static final int ATTEMPT = 0;
    private static final String PROCESSING = "PROCESSING";

    private final long timeout;

    StatusRetry(long timeout) {
        this.timeout = System.currentTimeMillis() + timeout;
    }

    @Override
    public long getBackoff(int retryNumber) {
        return DEFAULT_RETRY;
    }

    @Override
    protected boolean shouldRetryImpl(int retryNumber, Object response) {
        if (!(response instanceof PrismResponse)) {
            throw new ConnectorException(ERROR_BUCKET_STATUS);
        }
        boolean shouldRetry = ((PrismResponse) response).isSuccess() && (System.currentTimeMillis() < timeout)
                && isProcessing((PrismResponse) response);

        if (shouldRetry) {
            backoff(retryNumber);
        }
        return shouldRetry;
    }

    public boolean shouldRetry(PrismResponse response) {
        return shouldRetry(ATTEMPT, response);
    }

    /** Helper method to determine if the the retry is in progress
     * @param response
     * @return boolean
     */
    private static boolean isProcessing(PrismResponse response) {
        try {
            JsonNode payload = response.getJsonEntity();
            JsonNode status = payload.path(Constants.FIELD_STATE).path(Constants.FIELD_DESCRIPTOR);
            if (!status.isMissingNode()) {
                return PROCESSING.equalsIgnoreCase(status.asText());
            }
        }
        catch (IOException e) {
            LOG.log(Level.WARNING, e.getMessage(), e);
        }
        throw new ConnectorException(ERROR_BUCKET_STATUS);
    }
}
