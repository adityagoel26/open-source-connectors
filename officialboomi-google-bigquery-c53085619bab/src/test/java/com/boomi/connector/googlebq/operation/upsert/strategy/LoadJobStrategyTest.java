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
import com.boomi.connector.googlebq.operation.upsert.JsonLoadFactory;
import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.connector.googlebq.resource.JobResource;
import com.boomi.connector.googlebq.resource.ResumableResource;
import com.boomi.connector.googlebq.util.JsonResponseUtil;
import com.boomi.restlet.RestletUtil;
import com.boomi.restlet.client.ResponseUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = {
        LoadJobStrategy.class, ResponseUtil.class, JobStatusChecker.class, RestletUtil.class, JsonResponseUtil.class })
public class LoadJobStrategyTest {

    private static final String LOCATION = "Location";

    private final ResumableResource _resumableResourc = mock(ResumableResource.class);
    private final JobResource _jobResource = mock(JobResource.class);
    private final ObjectData _document = mock(ObjectData.class);
    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final DynamicPropertyMap _map = mock(DynamicPropertyMap.class);
    private final Response _response = mock(Response.class);
    private final JsonLoadFactory _factory = mock(JsonLoadFactory.class);

    @Before
    public final void setup() throws Exception {
        mockStatic(ResponseUtil.class);
        mockStatic(JobStatusChecker.class);
        mockStatic(RestletUtil.class);
        mockStatic(JsonResponseUtil.class);
        OperationContext operationContext = mock(OperationContext.class);
        when(_connection.getContext()).thenReturn(operationContext);
        when(operationContext.getOperationProperties()).thenReturn(mock(PropertyMap.class));

        whenNew(ResumableResource.class).withArguments(eq(_connection)).thenReturn(_resumableResourc);
        whenNew(JobResource.class).withArguments(eq(_connection)).thenReturn(_jobResource);
        whenNew(JsonLoadFactory.class).withArguments(eq(_map), anyString()).thenReturn(_factory);

        when(_document.getDynamicOperationProperties()).thenReturn(_map);
        when(_factory.toJsonNode()).thenReturn(mock(ObjectNode.class));
    }

    @Test
    public void shouldCorrectlyExecuteService() throws Exception {
        Job job = mock(Job.class);
        TimeoutRetry retry = mock(TimeoutRetry.class);

        when(_resumableResourc.executeResumableSessionStartRequest(any(ObjectNode.class))).thenReturn(_response);
        when(ResponseUtil.validateResponse(_response)).thenReturn(true);
        when(RestletUtil.getHttpHeader(_response, LOCATION)).thenReturn(LOCATION);
        when(_resumableResourc.executeResumableFileUploadRequest(LOCATION, _document)).thenReturn(_response);
        whenNew(Job.class).withArguments(any(Response.class)).thenReturn(job);
        when(job.isDoneAndSuccessful()).thenReturn(true);
        when(job.getJob()).thenReturn(mock(JsonNode.class));
        when(job.isError()).thenReturn(false);
        whenNew(TimeoutRetry.class).withArguments(anyLong()).thenReturn(retry);
        when(JobStatusChecker.checkJobStatus(job, retry, _jobResource)).thenReturn(job);

        LoadJobStrategy strategy = new LoadJobStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertTrue(response.isSuccess());
    }

    @Test
    public void shouldReturnErrorResponseStrategyResultWhenResumableSessionIsNotValid()
            throws GeneralSecurityException, IOException {
        String errorMessage = "No session available";
        when(_resumableResourc.executeResumableSessionStartRequest(any(ObjectNode.class))).thenReturn(_response);
        when(ResponseUtil.validateResponse(_response)).thenReturn(false);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(StrategyUtil.buildErrorJsonNode(errorMessage));

        LoadJobStrategy strategy = new LoadJobStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals(errorMessage, response.getErrorMessage());
    }

    @Test
    public void shouldReturnErrorStrategyResultWhenHeaderIsNotFound() throws GeneralSecurityException, IOException {
        String errorMessage = "Location header is missing";
        when(_resumableResourc.executeResumableSessionStartRequest(any(ObjectNode.class))).thenReturn(_response);
        when(ResponseUtil.validateResponse(_response)).thenReturn(true);
        when(RestletUtil.getHttpHeader(_response, LOCATION)).thenReturn(EMPTY_STRING);
        when(JsonResponseUtil.extractPayload(_response)).thenReturn(StrategyUtil.buildErrorJsonNode(errorMessage));

        LoadJobStrategy strategy = new LoadJobStrategy(_connection);
        BaseStrategyResult response = strategy.executeService(_document);

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals(errorMessage, response.getErrorMessage());
    }

    @Test
    public void shouldReturnLoadName() {
        LoadJobStrategy strategy = new LoadJobStrategy(_connection);
        assertEquals("load", strategy.getNodeName());
    }
}