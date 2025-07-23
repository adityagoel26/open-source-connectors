// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.operation.create;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.constant.Constants;
import com.boomi.salesforce.rest.properties.ConnectionProperties;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.salesforce.rest.request.RequestBuilder;
import com.boomi.salesforce.rest.request.SFURIBuilder;
import com.boomi.util.StreamUtil;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Collections;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SFRestCreateTest {

    private static final String MESSAGE = "Message";

    private final OperationResponse _operationResponse = mock(OperationResponse.class);
    private final ObjectData _objectData = mock(ObjectData.class);
    private final UpdateRequest _updateRequest = mock(UpdateRequest.class);
    private final Logger _logger = mock(Logger.class);
    private final SFRestConnection _connection = mock(SFRestConnection.class, RETURNS_DEEP_STUBS);
    private final ConnectionProperties _connectionProperties = mock(ConnectionProperties.class);
    private final OperationProperties _operationProperties = mock(OperationProperties.class);
    private RequestBuilder _requestBuilder;
    private final OperationContext _operationContext = mock(OperationContext.class, RETURNS_DEEP_STUBS);

    @BeforeEach
    public void setup() throws Exception {
        when(_objectData.getLogger()).thenReturn(_logger);
        when(_objectData.getData()).thenReturn(StreamUtil.EMPTY_STREAM);
        when(_objectData.getDataSize()).thenReturn(0L);
        when(_updateRequest.iterator()).thenReturn(Collections.emptyIterator());

        when(_connectionProperties.getURL()).thenReturn(new URI("https://um6.salesforce.com/services/data/v49.0"));
        when(_connectionProperties.getAuthenticationUrl()).thenReturn(
                new URI("https://login.salesforce.com/services/Soap/u/48.0"));
        when(_connection.getConnectionProperties()).thenReturn(_connectionProperties);
        when(_connection.getOperationProperties()).thenReturn(_operationProperties);
        _requestBuilder = new RequestBuilder(new SFURIBuilder(_connection.getConnectionProperties().getURL(),
                _connection.getConnectionProperties().getAuthenticationUrl()), _connection.getOperationProperties());
        when(_connection.getContext()).thenReturn(_operationContext);
    }

    @Test
    public void shouldCallGetConnection() {
        assertNotNull(new SFRestCreateOperation(_connection).getConnection());
    }

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestCreateOperation(_connection).executeUpdate(_updateRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }

    @Test
    public void shouldAddHeaderWhenGivenAssignmentRule() throws Exception {
        ClassicHttpRequest request = _requestBuilder.createRecord("Case", StreamUtil.EMPTY_STREAM, -1, "123ID");

        assertNotNull(request.getHeader(Constants.ASSIGNMENT_RULE_ID_REST_HEADER));
        assertEquals("123ID", request.getHeader(Constants.ASSIGNMENT_RULE_ID_REST_HEADER).getValue());
        assertEquals(Constants.ASSIGNMENT_RULE_ID_REST_HEADER,
                request.getHeader(Constants.ASSIGNMENT_RULE_ID_REST_HEADER).getName());
    }

    @Test
    public void shouldNotAddHeaderWhenNotGivenAssignmentRule() throws Exception {
        ClassicHttpRequest request = _requestBuilder.createRecord("Case", StreamUtil.EMPTY_STREAM, -1, "");

        assertNull(request.getHeader(Constants.ASSIGNMENT_RULE_ID_REST_HEADER));
    }

    @Test
    public void shouldUseCompositeRequestWhenBatchCountGreaterThan1() {
        when(_operationProperties.getBatchCount()).thenReturn(2L);
        when(_operationProperties.isBulkOperationAPI()).thenReturn(false);
        SFRestCreateOperation op = spy(new SFRestCreateOperation(_connection));

        op.executeUpdate(_updateRequest, _operationResponse);

        verify(op).executeRestCreate(_updateRequest, _operationResponse);
    }

    @Test
    public void shouldUseCompositeRequestWhenBatchCountEquals1() {
        when(_operationProperties.getBatchCount()).thenReturn(1L);
        when(_operationProperties.isBulkOperationAPI()).thenReturn(false);
        SFRestCreateOperation op = spy(new SFRestCreateOperation(_connection));

        op.executeUpdate(_updateRequest, _operationResponse);

        verify(op).executeRestCreate(_updateRequest, _operationResponse);
    }

    @Test
    public void shouldUseBulkAPIOverCompositeAPI() {
        when(_operationProperties.getBatchCount()).thenReturn(2L);
        when(_operationProperties.isBulkOperationAPI()).thenReturn(true);
        SFRestCreateOperation op = spy(new SFRestCreateOperation(_connection));

        op.executeUpdate(_updateRequest, _operationResponse);

        verify(op).executeUpdate(_updateRequest, _operationResponse);
    }
}
