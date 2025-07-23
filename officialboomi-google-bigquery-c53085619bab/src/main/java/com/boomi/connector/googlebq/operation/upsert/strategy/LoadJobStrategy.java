//Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.connector.googlebq.operation.job.JobStatusChecker;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.connector.googlebq.operation.upsert.JsonJobFactory;
import com.boomi.connector.googlebq.operation.upsert.JsonLoadFactory;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.ErrorResponseStrategyResult;
import com.boomi.connector.googlebq.resource.JobResource;
import com.boomi.connector.googlebq.resource.ResumableResource;
import com.boomi.restlet.RestletUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.StringUtil;

import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Class in charge of creating a Load {@link Job} calling {@link ResumableResource} to upload the metadata for the job,
 * and upload the input data to BigQuery, and wait for the job status by calling {@link
 * JobStatusChecker#checkJobStatus(Job, TimeoutRetry, JobResource)}  and add the response
 */
public class LoadJobStrategy extends BaseStrategy {

    private static final String LOCATION_HEADER = "Location";
    private static final String LOAD = "load";
    private static final String LOCATION_HEADER_ERROR = "Location header is missing";

    private final ResumableResource _resumableResource;
    private final String _projectId;

    public LoadJobStrategy(GoogleBqOperationConnection connection) {
        super(connection);
        _resumableResource = new ResumableResource(connection);
        _projectId = connection.getProjectId();
    }

    @Override
    public BaseStrategyResult executeService(ObjectData document) throws IOException, GeneralSecurityException {
        JsonJobFactory loadJson = new JsonLoadFactory(document.getDynamicOperationProperties(), _projectId);

        Response response = _resumableResource.executeResumableSessionStartRequest(loadJson.toJsonNode());

        if (!ResponseUtil.validateResponse(response)) {
            return ErrorResponseStrategyResult.create(response);
        }

        String locationUrl = RestletUtil.getHttpHeader(response, LOCATION_HEADER);

        if (StringUtil.isBlank(locationUrl)) {
            return ErrorResponseStrategyResult.create(response, LOCATION_HEADER_ERROR);
        }

        response = _resumableResource.executeResumableFileUploadRequest(locationUrl, document);

        return checkStrategyResult(response);
    }

    @Override
    public String getNodeName() {
        return LOAD;
    }
}
