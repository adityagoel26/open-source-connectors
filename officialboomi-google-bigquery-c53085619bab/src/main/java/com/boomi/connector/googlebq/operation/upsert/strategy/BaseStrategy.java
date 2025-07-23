// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.connector.googlebq.operation.job.JobStatusChecker;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.connector.googlebq.operation.upsert.PayloadResponseBuilder;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.ErrorResponseStrategyResult;
import com.boomi.connector.googlebq.resource.JobResource;

import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

/**
 * Common abstraction layer to provide a Strategy for the execution of multiples step for the Upsert operation
 */
public abstract class BaseStrategy {

    private static final long DEFAULT_TIMEOUT = TimeUnit.HOURS.toMillis(1L);

    private final JobResource _jobResource;
    private final long _requestTimeout;

    BaseStrategy(GoogleBqOperationConnection connection) {
        _requestTimeout = connection.getContext().getOperationProperties().getLongProperty(
                GoogleBqConstants.PROP_REQUEST_TIMEOUT, DEFAULT_TIMEOUT);
        _jobResource = new JobResource(connection);
    }

    /**
     * Execute the service call on each implemented class and return a new {@link BaseStrategyResult} with their
     * corresponding response
     *
     * @param document
     * @return
     */
    abstract BaseStrategyResult executeService(ObjectData document) throws IOException, GeneralSecurityException;

    /**
     * Return true if calling method {@link BaseStrategy#executeService(ObjectData)} is a successful response, passing
     * the response content to {@link PayloadResponseBuilder} to be used a node for the Payload response. In case of
     * unsuccess, false is returned and a templated message error is added to the  {@link PayloadResponseBuilder}
     *
     * @param builder
     * @param document
     * @return true if it was a successful response. Otherwise, return false.
     */
    public boolean execute(PayloadResponseBuilder builder, ObjectData document) {
        try {
            BaseStrategyResult result = executeService(document);
            builder.withResult(getNodeName(), result);
            if (!result.isSuccess() && document.getLogger().isLoggable(Level.FINE)) {
                document.getLogger().log(Level.FINE, result.getContent().toString());
            }
            return result.isSuccess();
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            document.getLogger().log(Level.WARNING, errorMessage, e);
            builder.withException(getNodeName(), errorMessage);
        }
        return false;
    }

    /**
     * Validate if {@link BaseStrategyResult} is successful then check the status of a Running Job by calling {@link
     * JobStatusChecker#checkJobStatus(Job, TimeoutRetry, JobResource)}, and return a new instance of {@link
     * BaseStrategyResult} with the current {@link Job} state. Otherwise, return the given {@link BaseStrategyResult}.
     *
     * @param result
     * @return a new instance of StrategyResult if the given one is success otherwise, return the given {@link
     * BaseStrategyResult}.
     */
    BaseStrategyResult checkStrategyResult(Response result) throws IOException, GeneralSecurityException {

        Job job = new Job(result);

        if (job.isError()) {
            return ErrorResponseStrategyResult.create(job.getErrorResponse());
        }
        Job resultJob = JobStatusChecker.checkJobStatus(job, new TimeoutRetry(_requestTimeout), _jobResource);

        if (resultJob.isError()) {
            return ErrorResponseStrategyResult.create(resultJob.getErrorResponse());
        }

        return BaseStrategyResult.createSuccessResult(resultJob);
    }

    /**
     * Get the node name to be used as field on the payload response node.
     *
     * @return name of the implemented strategy class
     */
    abstract String getNodeName();
}
