// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job;

import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.connector.googlebq.util.StatusUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;

import java.io.IOException;

/**
 * Class to represent a job in Google BigQuery. Any type of job is the one where the configuration in a job resource.
 * These types of jobs are Query, Load, Copy or Extract.
 */
public class Job {

    private static final String NODE_JOB_REFERENCE = "jobReference";
    private static final String NODE_JOB_ID = "jobId";
    private static final String NODE_STATUS = "status";
    private static final String NODE_STATE = "state";
    private static final String NODE_ERROR_RESULT = "errorResult";
    private static final String NODE_LOCATION = "location";

    private static final String JOB_STATE_DONE = "DONE";
    private final String _jobId;
    private final String _location;
    private final String _code;
    private final JsonNode _job;
    private final Response _response;
    private final boolean _isError;

    public Job(Response response) throws IOException {
        _isError = !ResponseUtil.validateResponse(response) || !response.isEntityAvailable();
        _response = response;
        _code = StatusUtil.getStatus(response);
        _job = _isError ? JSONUtil.newObjectNode() : JsonResponseUtil.extractPayload(response);
        _jobId = extractJobId(_job);
        _location = extractLocationFromJob(_job);
    }

    /**
     * Returns the job resource
     *
     * @return
     */
    public JsonNode getJob() {
        return _job;
    }

    public boolean isError() {
        return _isError;
    }

    /**
     * Extracts the job id by reading the "jobId" field in the job resource
     */
    private static String extractJobId(JsonNode job) {
        return job.path(NODE_JOB_REFERENCE).path(NODE_JOB_ID).asText();
    }

    /**
     * Extracts the job location by reading the "location" field in the job resource
     */
    private static String extractLocationFromJob(JsonNode job) {
        return job.path(NODE_JOB_REFERENCE).path(NODE_LOCATION).asText(StringUtil.EMPTY_STRING);
    }

    /**
     * gets the job id for this job
     *
     * @return
     */
    public String getJobId() {
        return _jobId;
    }

    /**
     * gets the job location for this job
     *
     * @return
     */
    public String getLocation() {
        return _location;
    }

    /**
     * Returns true if the job status is "DONE". Checks the job status by reading the status.state field in job
     * resource. Job status can be PENDING, RUNNING or DONE. The job status is DONE for jobs reported as success or
     * failure.
     *
     * @return
     */
    public boolean isJobDone() {
        String jobState = _job.path(NODE_STATUS).path(NODE_STATE).asText();
        return JOB_STATE_DONE.equalsIgnoreCase(jobState) || StringUtil.isEmpty(jobState);
    }

    /**
     * Returns true if a job is a SUCCESS. This is verified by checking if an errorResult is present in the job
     * resource.
     *
     * @return
     */
    public boolean isJobSuccessful() {
        return _job.path(NODE_STATUS).path(NODE_ERROR_RESULT).isMissingNode();
    }

    /**
     * Computes an error message by reading the errorResult in the job resource.
     *
     * @return
     */
    public String getErrorResultMessage() {
        JsonNode error = _job.path(NODE_STATUS).path(NODE_ERROR_RESULT);
        return String.format(GoogleBqConstants.ERROR_STREAM_DATA, error.path(NODE_LOCATION).textValue(),
                error.path("reason").textValue(), error.path("message").textValue());
    }

    /**
     * Return the error from BigQuery to use as Error Payload
     *
     * @return {@link Response} with the response from BigQuery
     */
    public Response getErrorResponse() {
        return _response;
    }

    /**
     * Returns true if a job is a DONE and SUCCESS. This is verified by checking if an errorResult is present in the job
     * resource.
     *
     * @return
     */
    public boolean isDoneAndSuccessful() {
        return !isError() && isJobDone() && isJobSuccessful();
    }

    /**
     * Return true if Job state is PENDING or RUNNING, otherwise, return false.
     *
     * @return
     */
    public boolean isPendingOrRunning() {
        return !isJobDone();
    }

    /**
     * Return the status code from the response.
     *
     * @return
     */
    public String getCode() {
        return _code;
    }
}
