//Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.connector.googlebq.operation.job.JobStatusChecker;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.connector.googlebq.operation.upsert.JsonJobFactory;
import com.boomi.connector.googlebq.operation.upsert.JsonQueryFactory;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.resource.JobResource;

import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;

/**
 * Class in charge of creating a Query {@link Job}, added into BigQuery, waiting for the job status  by calling {@link
 * JobStatusChecker#checkJobStatus(Job, TimeoutRetry, JobResource)}  and add the response
 */
public class QueryJobStrategy extends BaseStrategy {

    private static final String QUERY = "query";
    private final JobResource _jobResource;
    private final String _projectId;

    public QueryJobStrategy(GoogleBqOperationConnection connection) {
        super(connection);
        _jobResource = new JobResource(connection);
        _projectId = connection.getProjectId();
    }

    @Override
    public BaseStrategyResult executeService(ObjectData document) throws IOException, GeneralSecurityException {
        JsonJobFactory queryFactory = new JsonQueryFactory(document.getDynamicOperationProperties(), _projectId);

        Response response = _jobResource.insertJob(queryFactory.toJsonNode());
        return checkStrategyResult(response);
    }

    @Override
    public String getNodeName() {
        return QUERY;
    }
}