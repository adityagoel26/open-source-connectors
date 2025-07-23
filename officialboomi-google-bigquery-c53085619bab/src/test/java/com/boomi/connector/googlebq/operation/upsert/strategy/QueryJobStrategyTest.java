// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert.strategy;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.Job;
import com.boomi.connector.googlebq.operation.job.JobStatusChecker;
import com.boomi.connector.googlebq.operation.retry.TimeoutRetry;
import com.boomi.connector.googlebq.operation.upsert.JsonQueryFactory;
import com.boomi.connector.googlebq.operation.upsert.PayloadResponseBuilder;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.resource.JobResource;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.restlet.RestletUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.restlet.data.Response;

import java.io.IOException;
import java.security.GeneralSecurityException;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.powermock.api.mockito.PowerMockito.doNothing;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { QueryJobStrategy.class, ResponseUtil.class, JobStatusChecker.class, RestletUtil.class,
        JsonResponseUtil.class})
public class QueryJobStrategyTest {

    private final JobResource _jobResource = mock(JobResource.class);
    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final DynamicPropertyMap _map = mock(DynamicPropertyMap.class);
    private final ObjectData _document = mock(ObjectData.class);
    private final Response _response = mock(Response.class);
    private final JsonQueryFactory _factory = mock(JsonQueryFactory.class);
    private final JsonNode _node = mock(JsonNode.class);
    private final Job _job = mock(Job.class);

    @Before
    public final void setup() throws Exception {
        mockStatic(ResponseUtil.class);
        mockStatic(JobStatusChecker.class);
        mockStatic(RestletUtil.class);
        mockStatic(JsonResponseUtil.class);
        mockBaseStrategy();

        whenNew(JobResource.class).withArguments(eq(_connection)).thenReturn(_jobResource);
        whenNew(JsonQueryFactory.class).withArguments(eq(_map), anyString()).thenReturn(_factory);
        when(_factory.toJsonNode()).thenReturn(_node);
    }

    @Test
    public void shouldCorrectlyExecuteService() throws Exception {
        Job job = mock(Job.class);
        TimeoutRetry retry = mock(TimeoutRetry.class);

        when(_jobResource.insertJob(_node)).thenReturn(_response);
        whenNew(Job.class).withArguments(any(Response.class)).thenReturn(job);
        when(job.isError()).thenReturn(false);
        whenNew(TimeoutRetry.class).withArguments(anyLong()).thenReturn(retry);
        when(JobStatusChecker.checkJobStatus(job, retry, _jobResource)).thenReturn(job);
        when(job.isDoneAndSuccessful()).thenReturn(true);

        QueryJobStrategy strategy = new QueryJobStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertTrue(response.isSuccess());
    }

    @Test
    public void shouldReturnErrorStrategyResultWhenJobFail() throws GeneralSecurityException, IOException {
        when(_job.isError()).thenReturn(true, true);
        when(_job.getErrorResponse()).thenReturn(_response);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(JSONUtil.newObjectNode());
        when(ResponseUtil.getMessage(_response)).thenReturn("error job");

        QueryJobStrategy strategy = new QueryJobStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals("", response.getCode());
        assertEquals("error job", response.getErrorMessage());
    }

    @Test
    public void shouldReturnQueryName() {
        QueryJobStrategy strategy = new QueryJobStrategy(_connection);
        assertEquals("query", strategy.getNodeName());
    }

    private void mockBaseStrategy() throws Exception {
        when(_document.getDynamicOperationProperties()).thenReturn(_map);
        when(_connection.getProjectId()).thenReturn(EMPTY_STRING);
        OperationContext operationContext = mock(OperationContext.class);
        when(_connection.getContext()).thenReturn(operationContext);
        when(operationContext.getOperationProperties()).thenReturn(mock(PropertyMap.class));
        whenNew(Job.class).withArguments(any(Response.class)).thenReturn(_job);
    }

}