// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job.handler;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.resource.JobResource;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Common base class to validate the state of a Run Job and add the response to the operation
 */
public abstract class BaseJobHandler {

    protected final JobResource _jobResource;
    protected final long _requestTimeout;

    private static final long DEFAULT_TIMEOUT = 3600000L;
    private static final long MIN_TIMEOUT = -2L;

    protected BaseJobHandler(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        _jobResource = new JobResource(connection);
        _requestTimeout = connection.getContext().getOperationProperties().getLongProperty(
                GoogleBqConstants.PROP_REQUEST_TIMEOUT, DEFAULT_TIMEOUT);

        if (_requestTimeout <= MIN_TIMEOUT) {
            throw new ConnectorException("Request Timeout should be -1(indefinite) or greater");
        }
    }

    /**
     * Runs a new asynchronous job, and continues check the status from the Job. Error and Response are handled by each
     * implementation
     *
     * @param document
     * @param operationResponse
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public abstract void runJob(ObjectData document, OperationResponse operationResponse)
            throws IOException, GeneralSecurityException;
}
