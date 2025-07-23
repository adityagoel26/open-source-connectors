// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.resource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.util.StringUtil;
import com.boomi.util.URLUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class contains api methods supported by Google BigQuery for a Job resource.
 */
public class JobResource extends GoogleBqResource {

    private static final String PARAM_MAX_RESULTS = "maxResults";
    private static final String PARAM_TIMEOUT = "timeoutMs";
    private static final String PARAM_LOCATION = "location";

    private static final String PROJECTS_PATH = "projects";
    private static final String JOB_URL_SUFFIX = "jobs";
    private static final String QUERY_URL_SUFFIX = "queries";

    private static final int INITIAL_GET_MAP_CAPACITY = 4;
    private static final int INITIAL_GET_JOB_MAP_CAPACITY = 1;

    private final String _projectEndpoint;

    public JobResource(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        super(connection);
        _projectEndpoint = URLUtil.makeUrlString(PROJECTS_PATH, connection.getProjectId());
    }

    /**
     * Inserts a new job into BigQuery. This method will start a new asynchronous job. This method will return
     * immediately. {@link JobResource#getJob(String, String)} needs to be repeatedly called next to learn the updated
     * job status.
     *
     * @param job
     * @return
     * @throws IOException
     * @throws GeneralSecurityException
     */
    public Response insertJob(JsonNode job) throws GeneralSecurityException {
        String url = URLUtil.makeUrlString(_projectEndpoint, JOB_URL_SUFFIX);
        return executePost(url, job);
    }

    /**
     * Returns information about a specific job. This method can be called repeatedly to get the updated state of the
     * job,
     *
     * @param jobId
     * @param location
     * @return
     * @throws GeneralSecurityException
     */
    public Response getJob(String jobId, String location) throws GeneralSecurityException {
        String url = URLUtil.makeUrlString(_projectEndpoint, JOB_URL_SUFFIX, jobId);

        Map<String, String> params = new HashMap<>(INITIAL_GET_JOB_MAP_CAPACITY);
        addLocationParam(params, location);

        return executeGet(params, url);
    }

    /**
     * Executes a new GetQueryResults operation against the BigQuery API
     *
     * @param jobId
     *         the source jobId
     * @param location
     *         the source location
     * @param timeout
     *         the timeout property specified on the operation
     * @param maxResults
     *         the maximum amounts of rows to include be included on the response
     * @param pageToken
     *         the index to the desired page
     * @return a {@link Response} instance.
     * @throws GeneralSecurityException
     *         This should never happen
     */
    public Response getQueryResults(String jobId, String location, long timeout, long maxResults, String pageToken)
            throws GeneralSecurityException {
        String url = URLUtil.makeUrlString(_projectEndpoint, QUERY_URL_SUFFIX, jobId);

        Map<String, String> params = new HashMap<>(INITIAL_GET_MAP_CAPACITY);
        addLocationParam(params, location);
        params.put(PARAM_MAX_RESULTS, String.valueOf(maxResults));
        params.put(PARAM_TIMEOUT, String.valueOf(timeout));
        addPageToken(params, pageToken);

        return executeGZipCompressedGet(params, url);
    }

    private static void addLocationParam(Map<String, String> params, String location) {
        if (StringUtil.isNotBlank(location)) {
            params.put(PARAM_LOCATION, location);
        }
    }
}
