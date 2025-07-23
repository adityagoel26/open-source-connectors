// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.update.UpdateTable;
import com.boomi.connector.googlebq.resource.TableResource;
import com.boomi.util.CollectionUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.restlet.data.Response;
import org.restlet.data.Status;
import org.restlet.resource.Representation;

import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.connector.googlebq.GoogleBqTesUtils.getResource;
import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GoogleBqUpdateOperationTest {
    private static final String ERROR_CANNOT_PARSE = "Unable to parse input document as a valid json document";
    private static final String OBJECT_TYPE_ID = "datasets/Tango_testing/tables/IT_no_delete";

    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final OperationContext _context = mock(OperationContext.class);
    private final PropertyMap _props = mock(PropertyMap.class);
    private final TableResource _tableResource = mock(TableResource.class);
    private final ObjectData _data = mock(ObjectData.class);
    private final UpdateRequest _request = mock(UpdateRequest.class);
    private final OperationResponse _opResponse = mock(OperationResponse.class);
    private final Response _response = mock(Response.class);
    private final Representation _entity = mock(Representation.class);
    private final Logger _logger = mock(Logger.class);

    private final List<ObjectData> _inputDocs = CollectionUtil.asList(_data);

    private final GoogleBqUpdateOperation operation = new GoogleBqUpdateOperation(_connection, _tableResource);

    @Before
    public void setup() {
        when(_connection.getOperationContext()).thenReturn(_context);
        when(_request.iterator()).thenReturn(_inputDocs.iterator());
    }

    @After
    public void tearDown() throws Exception {
        verify(_request).iterator();

    }

    @Test
    public void shouldCorrectlyExecutePatch() throws Exception {
        InputStream document = prepareExec();
        when(_data.getLogger()).thenReturn(_logger);
        when(_data.getData()).thenReturn(document);
        when(_props.getBooleanProperty(GoogleBqConstants.PROP_IS_UPDATE, false)).thenReturn(false);
        when(_tableResource.patchTable(any(UpdateTable.class))).thenReturn(_response);
        when(_response.getEntity()).thenReturn(_entity);
        when(_response.isEntityAvailable()).thenReturn(true);
        when(_data.getLogger()).thenReturn(_logger);
        when(_entity.getStream()).thenReturn(getResource("view.json"));

        operation.executeUpdate(_request, _opResponse);
        commonVerifies();

        verify(_tableResource).patchTable(any(UpdateTable.class));
        verify(_connection, times(2)).getOperationContext();
        verify(_entity).getStream();
        verify(_opResponse).addResult(any(ObjectData.class), any(OperationStatus.class), anyString(), anyString(),
                any(Payload.class));
    }

    @Test
    public void shouldCorrectlyExecuteUpdate() throws Exception {
        InputStream document = prepareExec();
        when(_data.getLogger()).thenReturn(_logger);
        when(_data.getData()).thenReturn(document);
        when(_props.getBooleanProperty(GoogleBqConstants.PROP_IS_UPDATE, false)).thenReturn(true);
        when(_tableResource.updateTable(any(UpdateTable.class))).thenReturn(_response);
        when(_response.getEntity()).thenReturn(_entity);
        when(_response.isEntityAvailable()).thenReturn(true);
        when(_data.getLogger()).thenReturn(_logger);
        when(_entity.getStream()).thenReturn(getResource("view.json"));

        operation.executeUpdate(_request, _opResponse);
        commonVerifies();

        verify(_tableResource).updateTable(any(UpdateTable.class));
        verify(_connection, times(2)).getOperationContext();
        verify(_entity).getStream();
        verify(_opResponse).addResult(any(ObjectData.class), any(OperationStatus.class), anyString(), anyString(),
                any(Payload.class));
    }

    @Test
    public void shouldAddApplicationErrorForMissingResponseBody() throws Exception {
        InputStream document = prepareExec();
        when(_data.getLogger()).thenReturn(_logger);
        when(_data.getData()).thenReturn(document);
        when(_props.getBooleanProperty(GoogleBqConstants.PROP_IS_UPDATE, false)).thenReturn(false);
        when(_tableResource.patchTable(any(UpdateTable.class))).thenReturn(_response);
        when(_data.getLogger()).thenReturn(_logger);
        when(_response.isEntityAvailable()).thenReturn(false);
        operation.executeUpdate(_request, _opResponse);
        commonVerifies();

        verify(_tableResource).patchTable(any(UpdateTable.class));
        verify(_logger).log(any(Level.class), anyString(), any(ConnectorException.class));
        verify(_opResponse).addResult(_data, OperationStatus.APPLICATION_ERROR, "200", "there's no response body",
                null);
    }

    @Test
    public void shouldAddApplicationError() throws Exception {
        ConnectorException exception = new ConnectorException(ERROR_CANNOT_PARSE);
        when(_data.getLogger()).thenReturn(_logger);

        doThrow(exception).when(_data).getData();

        operation.executeUpdate(_request, _opResponse);

        verify(_data).getLogger();
        verify(_data).getData();
        verify(_opResponse).addResult(_data, OperationStatus.APPLICATION_ERROR, exception.getStatusCode(),
                exception.getMessage(), null);
    }

    @Test
    public void shouldAddExceptionFailure() throws Exception {
        RuntimeException exception = new RuntimeException(EMPTY_STRING);
        prepareExec();
        when(_data.getLogger()).thenReturn(_logger);
        doThrow(exception).when(_tableResource).patchTable(any(UpdateTable.class));

        operation.executeUpdate(_request, _opResponse);

        verify(_data).getLogger();
        verify(_data).getData();
        verify(_logger).log(Level.SEVERE, "Failed processing input " + _data, exception);
        verify(_opResponse).addErrorResult(_data, OperationStatus.FAILURE, EMPTY_STRING, "java.lang.RuntimeException: ",
                exception);

    }

    private InputStream prepareExec() throws Exception {
        InputStream document = getResource("view.json");
        Status status = Status.SUCCESS_OK;

        when(_data.getData()).thenReturn(document);
        when(_connection.getContext()).thenReturn(_context);
        when(_context.getOperationProperties()).thenReturn(_props);
        when(_context.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        when(_response.getStatus()).thenReturn(status);
        return document;
    }

    private void commonVerifies() throws Exception {
        verify(_data).getData();
        verify(_context).getOperationProperties();
        verify(_props).getBooleanProperty(GoogleBqConstants.PROP_IS_UPDATE, false);
        verify(_context).getObjectTypeId();
        verify(_connection, times(2)).getOperationContext();
        verify(_response).getStatus();
        verify(_response).isEntityAvailable();
    }
}
