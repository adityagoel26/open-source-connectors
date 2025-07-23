// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job.handler;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.GetQueryResultsProcessor;
import com.boomi.connector.googlebq.resource.JobResource;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

/**
 * Handler to perform operation specific to a GetQueryResultsJob. API resource
 * This handler is to be used to perform the retrieval of the results produced by a Query API Operation.
 *
 */
public class GetQueryResultsHandler {
    private static final long DEFAULT_TIMEOUT = 10000;
    private static final long DEFAULT_PAGE_SIZE = 1000;
    private static final String NODE_JOB_ID = "jobId";
    private static final String NODE_LOCATION = "location";

    private final JobResource _jobResource;
    private final long _timeout;
    private final long _maxResults;

    /**
     * Create a new instance of {@link GetQueryResultsHandler}.
     *
     * @param jobResource
     *         a {@link JobResource} instance.
     * @param timeOut
     *         a long value for the request timeout.
     * @param maxResults
     *         a long value for the page size.
     */
    GetQueryResultsHandler(JobResource jobResource, long timeOut, long maxResults) {
        _jobResource = jobResource;
        _timeout = timeOut;
        _maxResults = maxResults;
    }

    /**
     * Create a new instance of {@link GetQueryResultsHandler}.
     *
     * @param connection
     *         a {@link GoogleBqOperationConnection} instance.
     */
    public static GetQueryResultsHandler getInstance(GoogleBqOperationConnection connection) {
        return new GetQueryResultsHandler(new JobResource(connection), getTimeOut(connection),
                getMaxResults(connection));
    }

    /**
     * Runs a new GetQueryResults operations against the BigQuery API and includes the results into the
     * OperationResponse
     *
     * @param data
     *         an {@link ObjectData} instance received as input document.
     * @param opResponse
     *         an {@link OperationResponse} instance.
     */
    public void run(ObjectData data, OperationResponse opResponse) throws GeneralSecurityException {
        JsonNode properties = parseInput(data.getData());
        String jobId = getJobId(properties);
        String location = getLocationFromProperties(properties);

        String pageToken = null;
        boolean hasResults;

        do {
            Response response = _jobResource.getQueryResults(jobId, location, _timeout, _maxResults, pageToken);

            if (!ResponseUtil.validateResponse(response)) {
                addPartialApplicationError(data, opResponse, response);
                hasResults = true;
                break;
            }

            GetQueryResultsProcessor job = new GetQueryResultsProcessor(response);
            try {
                hasResults = job.process(data, opResponse);
            }
            catch (ConnectorException e) {
                data.getLogger().log(Level.WARNING, e.getMessage(), e);
                opResponse.addPartialResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(),
                        e.getStatusMessage(), null);
                hasResults = true;
                break;
            }
            pageToken = job.getNextPageToken();
        } while (StringUtil.isNotBlank(pageToken));

        if (hasResults) {
            opResponse.finishPartialResult(data);
        }
        else {
            opResponse.addEmptyResult(data, OperationStatus.SUCCESS, String.valueOf(Status.SUCCESS_OK.getCode()),
                    Status.SUCCESS_OK.getDescription());
        }

    }

    /**
     * Adds a partial application error for given data
     *
     * @param data
     *         an {@link ObjectData} instance.
     * @param opResponse
     *         an {@link OperationResponse} instance.
     */
    private static void addPartialApplicationError(ObjectData data, OperationResponse opResponse, Response response) {
        Status status = response.getStatus();

        InputStream stream = null;
        GZIPInputStream gzipStream = null;
        if (response.isEntityAvailable()) {
            try {
                stream = response.getEntity().getStream();
                gzipStream = new GZIPInputStream(stream);

                opResponse.addPartialResult(data, OperationStatus.APPLICATION_ERROR, String.valueOf(status.getCode()),
                        status.getDescription(), PayloadUtil.toPayload(gzipStream));
                return;
            }
            catch (Exception e) {
                data.getLogger().log(Level.WARNING, e.getMessage(), e);
            }
            finally {
                IOUtil.closeQuietly(stream, gzipStream);
            }
        }
        opResponse.addPartialResult(data, OperationStatus.APPLICATION_ERROR, String.valueOf(status.getCode()),
                status.getDescription(), null);

    }

    private static String getJobId(JsonNode properties) {
        JsonNode jobId = properties.path(NODE_JOB_ID);
        if (jobId.isMissingNode()) {
            throw new ConnectorException(String.valueOf(Status.CLIENT_ERROR_BAD_REQUEST.getCode()),
                    GoogleBqConstants.ERROR_PARSE_JSON);
        }
        return jobId.asText();
    }

    private static String getLocationFromProperties(JsonNode properties) {
        return properties.path(NODE_LOCATION).asText(StringUtil.EMPTY_STRING);
    }

    private static JsonNode parseInput(InputStream stream) {
        try {
            return JSONUtil.parseNode(stream);
        }
        catch (Exception e) {
            throw new ConnectorException(String.valueOf(Status.CLIENT_ERROR_BAD_REQUEST.getCode()),
                    GoogleBqConstants.ERROR_PARSE_JSON, e);
        }
        finally {
            IOUtil.closeQuietly(stream);
        }
    }

    private static long getTimeOut(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        return connection.getContext().getOperationProperties().getLongProperty(GoogleBqConstants.PROP_TIMEOUT,
                DEFAULT_TIMEOUT);
    }

    private static long getMaxResults(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        return connection.getContext().getOperationProperties().getLongProperty(
                GoogleBqConstants.PROP_MAX_RESULTS, DEFAULT_PAGE_SIZE);
    }
}
