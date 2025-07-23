// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.upsert.PayloadResponseBuilder;
import com.boomi.connector.googlebq.operation.upsert.strategy.DeleteStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.LoadJobStrategy;
import com.boomi.connector.googlebq.operation.upsert.strategy.QueryJobStrategy;
import com.boomi.util.CollectionUtil;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.boomi.util.StringUtil.EMPTY_STRING;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.doThrow;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { GoogleBqUpsertOperation.class })
public class GoogleBqUpsertOperationTest {

    private final GoogleBqOperationConnection _connection = mock(GoogleBqOperationConnection.class);
    private final OperationContext _context = mock(OperationContext.class);
    private final UpdateRequest _request = mock(UpdateRequest.class);
    private final OperationResponse _response = mock(OperationResponse.class);
    private final ObjectData _data = mock(ObjectData.class);
    private final LoadJobStrategy _loadJobStrategy = mock(LoadJobStrategy.class);
    private final QueryJobStrategy _queryJobStrategy = mock(QueryJobStrategy.class);
    private final DeleteStrategy _deleteStrategy = mock(DeleteStrategy.class);
    private final PayloadResponseBuilder _builder = mock(PayloadResponseBuilder.class);
    private final GoogleBqUpsertOperation _operation = new GoogleBqUpsertOperation(_connection);

    @Before
    public void setup() throws Exception {
        List<ObjectData> _inputDocs = CollectionUtil.asList(_data);
        InputStream _inputStream = mock(InputStream.class);

        when(_request.iterator()).thenReturn(_inputDocs.iterator());
        when(_data.getData()).thenReturn(_inputStream);
        when(_connection.getContext()).thenReturn(_context);

        whenNew(LoadJobStrategy.class).withArguments(_connection).thenReturn(_loadJobStrategy);
        whenNew(QueryJobStrategy.class).withArguments(_connection).thenReturn(_queryJobStrategy);
        whenNew(DeleteStrategy.class).withArguments(_connection).thenReturn(_deleteStrategy);
        whenNew(PayloadResponseBuilder.class).withNoArguments().thenReturn(_builder);
        when(_builder.toPayload()).thenReturn(mock(Payload.class));
    }

    @Test
    public void shouldCorrectlyExecuteUpdate() {
        when(_data.getDynamicOperationProperties()).thenReturn(mock(DynamicPropertyMap.class));
        when(_data.getDynamicOperationProperties().getProperty("runSqlAfterLoad")).thenReturn("true");
        when(_data.getDynamicOperationProperties().getProperty("deleteTempTableAfterQuery")).thenReturn(
                "DELETE_ALWAYS_VALUE");
        when(_loadJobStrategy.execute(_builder, _data)).thenReturn(true);
        when(_queryJobStrategy.execute(_builder, _data)).thenReturn(true);
        when(_deleteStrategy.execute(_builder, _data)).thenReturn(true);

        _operation.executeUpdate(_request, _response);

        verify(_response).addResult(eq(_data), eq(OperationStatus.SUCCESS), eq("Success"), isNull(String.class),
                any(Payload.class));
    }

    @Test
    public void shouldCorrectlyExecuteOnlyLoad() {
        when(_data.getDynamicOperationProperties()).thenReturn(mock(DynamicPropertyMap.class));
        when(_data.getDynamicOperationProperties().getProperty("runSqlAfterLoad")).thenReturn("false");
        when(_data.getDynamicOperationProperties().getProperty("deleteTempTableAfterQuery")).thenReturn(
                "NO_DELETE_VALUE");
        when(_loadJobStrategy.execute(_builder, _data)).thenReturn(true);
        when(_queryJobStrategy.execute(_builder, _data)).thenReturn(false);

        _operation.executeUpdate(_request, _response);

        verify(_response).addResult(eq(_data), eq(OperationStatus.SUCCESS), eq("Success"), isNull(String.class),
                any(Payload.class));
    }

    @Test
    public void shouldBuildWithErrorWhenDeleteStepFails() throws Exception {
        arrangeBuilderWithError("DELETE_IF_SUCCESS_VALUE", true, true, false);

        _operation.executeUpdate(_request, _response);

        verify(_response).addResult(eq(_data), eq(OperationStatus.APPLICATION_ERROR), eq(ConnectorException.NO_CODE),
                anyString(), any(Payload.class));
    }

    @Test
    public void shouldThrowExceptionWhenLoadStrategyFail() {
        Logger logger = mock(Logger.class);
        when(_data.getLogger()).thenReturn(logger);
        RuntimeException exception = new RuntimeException(EMPTY_STRING);
        doThrow(exception).when(_loadJobStrategy).execute(_builder, _data);

        _operation.executeUpdate(_request, _response);

        verify(_data).getLogger();
        verify(logger).log(Level.SEVERE, "Failed processing input " + _data, exception);
        verify(_request, Mockito.times(2)).iterator();
        verify(_response).addErrorResult(_data, OperationStatus.FAILURE, EMPTY_STRING, "java.lang.RuntimeException: ",
                exception);
    }

    @Test
    public void shouldBuildWithErrorWhenQueryAndDeleteStepFails() throws Exception {
        arrangeBuilderWithError("DELETE_ALWAYS_VALUE", true, false, false);

        _operation.executeUpdate(_request, _response);

        verify(_response).addResult(eq(_data), eq(OperationStatus.APPLICATION_ERROR), eq(EMPTY_STRING),
                eq(EMPTY_STRING), any(Payload.class));
    }

    private void arrangeBuilderWithError(String deleteTable, boolean load, boolean query, boolean delete)
            throws Exception {
        when(_data.getDynamicOperationProperties()).thenReturn(mock(DynamicPropertyMap.class));
        when(_data.getDynamicOperationProperties().getProperty("runSqlAfterLoad")).thenReturn("true");
        when(_data.getDynamicOperationProperties().getProperty("deleteTempTableAfterQuery")).thenReturn(deleteTable);
        when(_loadJobStrategy.execute(_builder, _data)).thenReturn(load);
        when(_queryJobStrategy.execute(_builder, _data)).thenReturn(query);
        when(_deleteStrategy.execute(_builder, _data)).thenReturn(delete);
        PayloadResponseBuilder builder = mock(PayloadResponseBuilder.class);
        whenNew(PayloadResponseBuilder.class).withNoArguments().thenReturn(builder);
        when(builder.getMessage()).thenReturn(EMPTY_STRING);
        when(builder.getCode()).thenReturn(EMPTY_STRING);
    }
}