// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.job;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;

import org.junit.Test;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

import java.io.IOException;

import static com.boomi.connector.googlebq.GoogleBqTesUtils.getCompressedResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Gaston Cruz <gaston.cruz@boomi.com>
 */
public class GetQueryResultsProcessorTest {
    private static final String RES_RESULT_WITHOUT_PAGE_TOKEN = "results.json";
    private static final String RES_UNSORTED_RESULTS = "unsorted_results.json";
    private static final String RES_EMPTY_RESULTS = "empty_results.json";

    private final Response _response = mock(Response.class);
    private final Representation _representation = mock(Representation.class);
    private final ObjectData _data = mock(ObjectData.class);
    private final OperationResponse _opResponse = mock(OperationResponse.class);

    @Test
    public void shouldCorrectlyParseStandardResponse() throws IOException {
        when(_representation.getStream()).thenReturn(getCompressedResource(RES_RESULT_WITHOUT_PAGE_TOKEN));
        runandAssertSuccessfulResponse();

        verify(_opResponse, times(3)).addPartialResult(any(ObjectData.class), any(OperationStatus.class), anyString(),
                anyString(), any(Payload.class));
    }

    @Test
    public void shouldCorrectlyParseEmptyResponse() throws IOException {
        when(_representation.getStream()).thenReturn(getCompressedResource(RES_EMPTY_RESULTS));
        runandAssertSuccessfulResponse();
    }

    private void runandAssertSuccessfulResponse() throws IOException {
        Status status = Status.SUCCESS_OK;

        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_representation);

        GetQueryResultsProcessor proc = new GetQueryResultsProcessor(_response);
        proc.process(_data, _opResponse);
        assertNull(proc.getNextPageToken());

        verify(_response).getStatus();
        verify(_response).getEntity();
        verify(_representation).getStream();
    }

    @Test
    public void shouldCorrectlyParseUnsortedResponse() throws IOException {
        Status status = Status.SUCCESS_OK;

        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_representation);
        when(_representation.getStream()).thenReturn(getCompressedResource(RES_UNSORTED_RESULTS));

        GetQueryResultsProcessor proc = new GetQueryResultsProcessor(_response);
        proc.process(_data, _opResponse);
        assertEquals("3456", proc.getNextPageToken());

        verify(_response).getStatus();
        verify(_response).getEntity();
        verify(_representation).getStream();
        verify(_opResponse, times(3)).addPartialResult(any(ObjectData.class), eq(OperationStatus.SUCCESS), anyString(),
                anyString(), any(Payload.class));
        verify(_opResponse, times(1)).addPartialResult(any(ObjectData.class), eq(OperationStatus.APPLICATION_ERROR),
                eq("202"),
                eq("message1"),
                any(Payload.class));
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenInvalidPayload() throws IOException {
        Status status = Status.SUCCESS_OK;
        when(_response.getStatus()).thenReturn(status);
        when(_response.getEntity()).thenReturn(_representation);
        doThrow(new IOException()).when(_representation).getStream();


        GetQueryResultsProcessor proc = new GetQueryResultsProcessor(_response);
        proc.process(_data, _opResponse);

        verify(_response).getStatus();
        verify(_response).getEntity();
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenEmptyPayload() throws IOException {
        Status status = Status.SUCCESS_OK;
        when(_response.getStatus()).thenReturn(status);

        GetQueryResultsProcessor proc = new GetQueryResultsProcessor(_response);
        proc.process(_data, _opResponse);

        verify(_response).getStatus();
        verify(_response).getEntity();
    }
}
