// Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.googlebq.resource;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.request.GoogleBqRequestFactory;
import com.boomi.connector.googlebq.request.ResumableUploadRequestFactory;
import com.boomi.restlet.client.RequestFactory;
import com.boomi.restlet.resource.JsonRepresentation;
import com.boomi.util.IOUtil;
import com.boomi.util.retry.RetryStrategy;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Response;
import org.restlet.resource.InputRepresentation;

import java.io.InputStream;
import java.security.GeneralSecurityException;

/**
 * This class is responsible to upload binary file into Google Big Query.
 */
public class ResumableResource extends GoogleBqResource {

    private static final String RESUMABLE_URL =
            "https://www.googleapis.com/upload/bigquery/v2/projects/%s/jobs?uploadType=resumable";
    private GoogleBqBaseConnection<? extends BrowseContext> _connection;
    private final String _resumableUrl;

    public ResumableResource(GoogleBqBaseConnection<? extends BrowseContext> connection) {
        super(connection);
        _connection = connection;
        _resumableUrl = String.format(RESUMABLE_URL, _connection.getProjectId());
    }

    /**
     * Creates an initial request to the upload URI that includes the metadata, if any. This method call to {@link
     * GoogleBqResource#executeRequest(RequestFactory, RetryStrategy)} to upload the metadata and obtain from the
     * response a "Location" Header
     *
     * @param job
     * @return
     * @throws GeneralSecurityException
     */
    public Response executeResumableSessionStartRequest(JsonNode job) throws GeneralSecurityException {
        RequestFactory requestFactory = new GoogleBqRequestFactory(_connection, Method.POST, _resumableUrl,
                new JsonRepresentation(job));
        return executeRequest(requestFactory, null);
    }

    /**
     * Upload a binary file into Big Query. Execute a Resumable upload making a new HTTP requests and sending it via
     * Restlet.
     *
     * @param locationUrl
     * @param data
     * @return
     * @throws GeneralSecurityException
     */
    public Response executeResumableFileUploadRequest(String locationUrl, ObjectData data)
            throws GeneralSecurityException {
        InputStream binaryData = null;
        try {
            binaryData = data.getData();
            return executeRequest(new ResumableUploadRequestFactory(_connection, locationUrl,
                    new InputRepresentation(binaryData, MediaType.ALL)));
        } finally {
            IOUtil.closeQuietly(binaryData);
        }
    }
}