// Copyright (c) 2019 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job.handler;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.googlebq.connection.GoogleBqBaseConnection;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.resource.JobResource;

import org.junit.Test;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

import java.security.GeneralSecurityException;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.connector.api.OperationStatus.APPLICATION_ERROR;
import static com.boomi.connector.googlebq.GoogleBqTesUtils.getCompressedResource;
import static com.boomi.connector.googlebq.GoogleBqTesUtils.getResource;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class GetQueryResultsHandlerTest {
    private static final String RES_JOB_ID = "job_id.json";
    private static final String RES_JOB_INPUT = "job_input.json";
    private static final String RES_RESULTS = "results.json";
    private static final String RES_EMPTY_RESULTS = "empty_results.json";
    private static final String RES_ERRORS = "query_error.json";
    private static final String JOB_ID = "job_IQxPnGzjvhnk7KmsCrBb7JWIqjBM";
    private static final String JOB_ID_ASIA_SOUTH = "job_I73DA9E-O_ROVgyNh_5tFN9h0kSt";
    private static final String LOCATION = "";
    private static final String LOCATION_ASIA_SOUTH = "asia-south1";
    private static final long TIMEOUT = 10000;
    private static final long MAX_RESULTS = 10;
    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final OperationContext _context = mock(OperationContext.class);
    private final PropertyMap _props = mock(PropertyMap.class);
    private final JobResource _jobResource = mock(JobResource.class);
    private final ObjectData _data = mock(ObjectData.class);
    private final OperationResponse _opResponse = mock(OperationResponse.class);
    private final Response _response = mock(Response.class);
    private final Representation _entity = mock(Representation.class);
    private final Logger _logger = mock(Logger.class);


    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenMissingInput() throws Exception {
        when(_data.getData()).thenReturn(getResource("view.json"));
        when(_connection.getContext()).thenReturn(_context);
        when(_context.getOperationProperties()).thenReturn(_props);

        GetQueryResultsHandler handler = GetQueryResultsHandler.getInstance(_connection);
        handler.run(_data, _opResponse);

        verify(_connection).getContext();
        verify(_context).getOperationProperties();
        verify(_data).getData();
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenInvalidInput() throws GeneralSecurityException {

        when(_connection.getContext()).thenReturn(_context);
        when(_context.getOperationProperties()).thenReturn(_props);

        GetQueryResultsHandler handler = GetQueryResultsHandler.getInstance(_connection);
        handler.run(_data, _opResponse);

        verify(_connection).getContext();
        verify(_context).getOperationProperties();
        verify(_data).getData();
    }

    @Test
    public void shouldReturnApplicationErrorWithoutResult() throws GeneralSecurityException {
        Status status = Status.CLIENT_ERROR_NOT_FOUND;

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_response.getStatus()).thenReturn(status);
        when(_response.isEntityAvailable()).thenReturn(false);

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).isEntityAvailable();
        verify(_opResponse).addPartialResult(_data, APPLICATION_ERROR, String.valueOf(status.getCode()),
                status.getDescription(), null);
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnApplicationErrorWithoutResultWhenRegionsOutsideUSandEU() throws GeneralSecurityException {
        Status status = Status.CLIENT_ERROR_NOT_FOUND;

        when(_jobResource.getQueryResults(JOB_ID_ASIA_SOUTH, LOCATION_ASIA_SOUTH, TIMEOUT, MAX_RESULTS, null))
                .thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_INPUT));
        when(_response.getStatus()).thenReturn(status);
        when(_response.isEntityAvailable()).thenReturn(false);

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID_ASIA_SOUTH, LOCATION_ASIA_SOUTH, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).isEntityAvailable();
        verify(_opResponse).addPartialResult(_data, APPLICATION_ERROR, String.valueOf(status.getCode()),
                status.getDescription(), null);
        verify(_opResponse).finishPartialResult(_data);

    }

    @Test
    public void shouldReturnApplicationErrorWithResultPayload() throws Exception {
        Status status = Status.CLIENT_ERROR_BAD_REQUEST;

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_response.getStatus()).thenReturn(status);
        when(_response.isEntityAvailable()).thenReturn(true);
        when(_response.getEntity()).thenReturn(_entity);
        when(_entity.getStream()).thenReturn(getCompressedResource(RES_ERRORS));

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).isEntityAvailable();
        verify(_response).getEntity();
        verify(_entity).getStream();
        verify(_opResponse).addPartialResult(any(ObjectData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnApplicationErrorWithInvalidResultPayload() throws Exception {
        Status status = Status.CLIENT_ERROR_BAD_REQUEST;
        RuntimeException exception = new RuntimeException("test");

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_data.getLogger()).thenReturn(_logger);
        when(_response.getStatus()).thenReturn(status);
        when(_response.isEntityAvailable()).thenReturn(true);
        when(_response.getEntity()).thenReturn(_entity);
        doThrow(exception).when(_entity).getStream();

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_data).getLogger();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).isEntityAvailable();
        verify(_response).getEntity();
        verify(_entity).getStream();
        verify(_logger).log(Level.WARNING, exception.getMessage(), exception);
        verify(_opResponse).addPartialResult(_data, APPLICATION_ERROR, String.valueOf(status.getCode()),
                status.getDescription(), null);
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnApplicationErrorWhenInvalidResponsePayload() throws Exception {
        Status status = Status.SUCCESS_OK;
        RuntimeException exception = new RuntimeException("could not parse response body");

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_data.getLogger()).thenReturn(_logger);
        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_entity);
        when(_data.getLogger()).thenReturn(_logger);
        doThrow(exception).when(_entity).getStream();

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_data).getLogger();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).getEntity();
        verify(_entity).getStream();
        verify(_logger).log(any(Level.class), anyString(), any(Throwable.class));
        verify(_opResponse).addPartialResult(_data, APPLICATION_ERROR, String.valueOf(status.getCode()),
                "could not parse response body", null);
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnSuccessWithQueryResults() throws Exception {
        Status status = Status.SUCCESS_OK;

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_entity);
        when(_entity.getStream()).thenReturn(getCompressedResource(RES_RESULTS));

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).getEntity();
        verify(_entity).getStream();

        verify(_opResponse, times(3)).addPartialResult(any(ObjectData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnSuccessWithQueryResultsWhenRegionOutsideUSandEU() throws Exception {
        Status status = Status.SUCCESS_OK;

        when(_jobResource.getQueryResults(JOB_ID_ASIA_SOUTH, LOCATION_ASIA_SOUTH, TIMEOUT, MAX_RESULTS, null))
                .thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_INPUT));
        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_entity);
        when(_entity.getStream()).thenReturn(getCompressedResource(RES_RESULTS));

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID_ASIA_SOUTH, LOCATION_ASIA_SOUTH, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).getEntity();
        verify(_entity).getStream();

        verify(_opResponse, times(3)).addPartialResult(any(ObjectData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
        verify(_opResponse).finishPartialResult(_data);
    }

    @Test
    public void shouldReturnSuccessWithEmptyResults() throws Exception {
        Status status = Status.SUCCESS_OK;

        when(_jobResource.getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null)).thenReturn(_response);
        when(_data.getData()).thenReturn(getResource(RES_JOB_ID));
        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_entity);
        when(_entity.getStream()).thenReturn(getCompressedResource(RES_EMPTY_RESULTS));

        GetQueryResultsHandler handler = new GetQueryResultsHandler(_jobResource, TIMEOUT, MAX_RESULTS);
        handler.run(_data, _opResponse);

        verify(_data).getData();
        verify(_jobResource).getQueryResults(JOB_ID, LOCATION, TIMEOUT, MAX_RESULTS, null);
        verify(_response, times(3)).getStatus();
        verify(_response).getEntity();
        verify(_entity).getStream();

        verify(_opResponse).addEmptyResult(_data, OperationStatus.SUCCESS, String.valueOf(status.getCode()),
                status.getDescription());
    }


}
