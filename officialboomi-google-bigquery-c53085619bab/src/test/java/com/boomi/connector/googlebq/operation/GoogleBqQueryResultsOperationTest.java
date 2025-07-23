// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.handler.GetQueryResultsHandler;
import com.boomi.util.CollectionUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GoogleBqQueryResultsOperationTest {

    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final GetQueryResultsHandler _handler = mock(GetQueryResultsHandler.class);
    private final ObjectData _data = mock(ObjectData.class);
    private final UpdateRequest _request = mock(UpdateRequest.class);
    private final OperationResponse _opResponse = mock(OperationResponse.class);
    private final Logger _logger = mock(Logger.class);

    private final List<ObjectData> _inputDocs = CollectionUtil.asList(_data);

    private final GoogleBqQueryResultsOperation operation = new GoogleBqQueryResultsOperation(_connection, _handler);

    @Before
    public void setup() {
        when(_request.iterator()).thenReturn(_inputDocs.iterator());
    }

    @After
    public void tearDown() throws Exception {
        verify(_request).iterator();
        verify(_handler).run(_data, _opResponse);
    }

    @Test
    public void shouldExecuteCorrectly() throws Exception {
        operation.executeUpdate(_request, _opResponse);
        verify(_handler).run(_data, _opResponse);
    }

    @Test
    public void shouldAddApplicationError() throws Exception {
        ConnectorException exception = new ConnectorException(EMPTY_STRING);
        when(_data.getLogger()).thenReturn(_logger);
        doThrow(exception).when(_handler).run(_data, _opResponse);

        operation.executeUpdate(_request, _opResponse);

        verify(_data).getLogger();
        verify(_logger).log(Level.WARNING, exception.getMessage(), exception);
        verify(_opResponse).addResult(_data, OperationStatus.APPLICATION_ERROR, exception.getStatusCode(),
                exception.getMessage(), null);
    }

    @Test
    public void shouldAddExceptionFailure() throws Exception {
        RuntimeException exception = new RuntimeException(EMPTY_STRING);
        when(_data.getLogger()).thenReturn(_logger);
        doThrow(exception).when(_handler).run(_data, _opResponse);

        operation.executeUpdate(_request, _opResponse);

        verify(_data).getLogger();
        verify(_logger).log(Level.SEVERE, "Failed processing input " + _data, exception);
        verify(_opResponse).addErrorResult(_data, OperationStatus.FAILURE, EMPTY_STRING, "java.lang.RuntimeException: ",
                exception);

    }
}
