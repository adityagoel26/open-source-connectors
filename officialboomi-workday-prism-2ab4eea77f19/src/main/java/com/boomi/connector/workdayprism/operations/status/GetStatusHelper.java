//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations.status;

import com.boomi.connector.workdayprism.PrismConnection;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.util.IOUtil;
import java.io.IOException;

/**
 * Delegate class in charge of making multiple requests to the Get Bucket endpoint in order to get the final "Success"
 * or "Failed" status once the bucket has been completed and its content is being processed.
 *
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GetStatusHelper {

    private final PrismOperationConnection connection;
    private final StatusRetry retry;

    /**
     * Creates a new {@link GetStatusHelper} instance
     *
     * @param connection
     *         an PrismConnection instance
     */
    public GetStatusHelper(PrismOperationConnection connection, long timeout) {
        this.connection = connection;
        this.retry = new StatusRetry(System.currentTimeMillis() + timeout);
    }

    /**
     * Returns the final bucket status
     *
     * @param bucketId
     *         an PrismConnection instance
     * @return a {@link PrismResponse} instance
     */
    public PrismResponse getStatus(String bucketId) throws IOException {
        PrismResponse response= null;
        do {
            IOUtil.closeQuietly(response);
            response = connection.getBucket(bucketId);
        } while (retry.shouldRetry(response));

        return response;
    }

}
