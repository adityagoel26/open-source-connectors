// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job.handler;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.JsonPayloadUtil;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.connector.googlebq.operation.job.JobStatusChecker;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

/**
 * Handler to perform operation specific to a {@link Job}. This handler is to be used to perform operations such as
 * retrieving the results of a {@link Job} once the job is done
 */
public class RunJobHandler extends BaseJobHandler {

    public RunJobHandler(GoogleBqOperationConnection connection) {
        super(connection);
    }

    /**
     * Runs a new asynchronous job. A job when created returns immediately with job state as RUNNING. {@link
     * BaseJobHandler#checkJobStatus(Job)}  is called to examine the job status. This method returns when the job state
     * is DONE or the call to check job status failed.
     * <p>
     * Once the job state is DONE and there are no errors a success result is added with {@link Job#getJob()} as the
     * payload. If the job failed there is an error result present which is used create an error message and an
     * application error is added.
     * <p>
     * If the call to check job status fails {@link RunJobHandler#addJobResult(Job, OperationResponse, ObjectData)} will
     * add an error result for the input document by calling {@link RunJobHandler#addFailedRequestResult(TrackedData,
     * Response, OperationResponse)} . In this case the {@link Job#isJobDone()} will be false.
     *
     * @param document
     * @param operationResponse
     */
    @Override
    public void runJob(ObjectData document, OperationResponse operationResponse)
            throws IOException, GeneralSecurityException {
        Response response = _jobResource.insertJob(createRequestPayload(document));

        Job job = new Job(response);

        if (job.isError()) {
            addFailedRequestResult(document, response, operationResponse);
        } else {
            Job jobResult = JobStatusChecker.checkJobStatus(job, new TimeoutRetry(_requestTimeout), _jobResource);
            addJobResult(jobResult, operationResponse, document);
        }
    }

    private static JsonNode createRequestPayload(ObjectData document) {
        InputStream stream = null;
        try {
            stream = document.getData();
            return JSONUtil.parseNode(stream);
        } catch (IOException e) {
            throw new ConnectorException(GoogleBqConstants.ERROR_PARSE_JSON, e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    /**
     * Adds result for a job. The implementation adds the job summary as a payload to the result for a successful job
     *
     * If {@link Job#isJobSuccessful()} is false an {@link OperationStatus#APPLICATION_ERROR} is added as a result for
     * the job with status as Status.CLIENT_ERROR_BAD_REQUEST.
     *
     * @param job
     * @param operationResponse
     * @param document
     */
    private static void addJobResult(Job job, OperationResponse operationResponse, ObjectData document) {
        if (job.isDoneAndSuccessful()) {
            com.boomi.connector.api.ResponseUtil.addSuccess(operationResponse, document,
                    Integer.toString(Status.SUCCESS_OK.getCode()), JsonPayloadUtil.toPayload(job.getJob()));
        } else if (job.isError() || job.isPendingOrRunning()) {
            addFailedRequestResult(document, job.getErrorResponse(), operationResponse);
        } else {
            // Job DONE with errors
            operationResponse.addResult(document, OperationStatus.APPLICATION_ERROR,
                    Integer.toString(Status.CLIENT_ERROR_BAD_REQUEST.getCode()), job.getErrorResultMessage(),
                    JsonPayloadUtil.toPayload(job.getJob()));
        }
    }

    /**
     * Adds an error result for given job. The error payload is the payload present in the error response.
     *
     * @param document
     * @param response
     */
    private static void addFailedRequestResult(TrackedData document, Response response,
            OperationResponse operationResponse) {
        Status status = response.getStatus();
        com.boomi.connector.api.ResponseUtil.addResultWithHttpStatus(operationResponse, document, status.getCode(),
                status.getDescription(), ResponseUtil.toErrorPayload(response, document.getLogger()));
    }
}
