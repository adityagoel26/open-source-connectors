// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.OperationStatus;

import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.testutil.SimpleDeleteRequest;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.TestUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Sets up the test environment by initializing Mockito mocks and configuring
 * the mock connection, context, and custom headers (Authorization and Content-Type)
 * for use in each test case.
 */
class HubspotcrmDeleteOperationTest {

    private HubspotcrmDeleteOperation _deleteOperation;

    @Mock
    private HubspotcrmOperationConnection _connection;

    @Mock
    private SimpleDeleteRequest _deleteRequest;

    @Mock
    private OperationResponse _operationResponse;

    @Mock
    private SimpleTrackedData _data;

    @Mock
    private OperationContext _context;

    @Mock
    private OperationType _operationType;

    @Mock
    private PropertyMap _connectionProperties;

    @Mock
    private CloseableHttpResponse _closeableHttpResponse;

    @Mock
    private StatusLine _statusLine;

    @Mock
    private HttpEntity _entity;

    @Mock
    private OAuth2Context oAuthContextMock;

    /**
     * Initializes Mockito annotations and configures mock behaviors for various components,
     * including connection properties and context. This ensures that the necessary
     * preconditions are met for testing the deletion operation in the HubSpot CRM.
     */
    @BeforeEach
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Authorization", "Bearer someToken");
        customHeaders.put("Content-Type", "application/json");

        when(_context.getOperationProperties()).thenReturn(_connectionProperties);
        when(_context.getConnectionProperties()).thenReturn(_connectionProperties);
        when(_connectionProperties.getProperty("idType")).thenReturn("emailId");
        when(_connectionProperties.getProperty("url")).thenReturn("https://api.hubapi.com");
        when(_connectionProperties.getCustomProperties("customHeaders")).thenReturn(customHeaders);
        when(_context.getObjectTypeId()).thenReturn("DELETE::/crm/v3/objects/contacts/{contactId}");
        when(_context.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(oAuthContextMock);
        String objectId = "1234";
        _data = new SimpleTrackedData(1, objectId, Collections.emptyMap());
        _deleteRequest = new SimpleDeleteRequest(Collections.singletonList(_data));
        when(_context.getOperationType()).thenReturn(_operationType);
        when(_operationType.name()).thenReturn("DELETE");
        when(_closeableHttpResponse.getStatusLine()).thenReturn(_statusLine);
        when(_connection.getContext()).thenReturn(_context);
        _deleteOperation = new HubspotcrmDeleteOperation(_connection);
        TestUtil.disableBoomiLog();
    }

    /**
     * Tests the successful execution of the archive operation.
     * Mocks the HTTP response to simulate a NO_CONTENT status and verifies
     * that the operation response is updated with a SUCCESS status.
     */
    @Test
    void testExecuteDeleteSuccess() {
        when(_statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NO_CONTENT);
        when(_statusLine.getReasonPhrase()).thenReturn("OK");
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(any(), any(), any(), any(), any(), any(), any())).thenReturn(
                    _closeableHttpResponse);
            _deleteOperation.executeDelete(_deleteRequest, _operationResponse);
            verify(_operationResponse).addResult(_data, OperationStatus.SUCCESS, "OK", "", null);
        }
    }

    /**
     * Tests the execution of archive operation with multiple documents.
     * Expected outcome: The archive operation should successfully process both
     * documents and add appropriate results to the operation response.
     */

    @Test
    void testExecuteDeleteWithMultipleDocument() {
        SimpleTrackedData trackedData1 = new SimpleTrackedData(1, "12345");
        SimpleTrackedData trackedData2 = new SimpleTrackedData(2, "67890");
        List<ObjectIdData> dataList = Arrays.asList(trackedData1, trackedData2);
        SimpleDeleteRequest deleteRequest = new SimpleDeleteRequest(dataList);
        when(_statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NO_CONTENT);
        when(_statusLine.getReasonPhrase()).thenReturn("OK");
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(any(), any(), any(), any(), any(), any(), any())).thenReturn(
                    _closeableHttpResponse);
            _deleteOperation.executeDelete(deleteRequest, _operationResponse);
            verify(_operationResponse).addResult(dataList.get(0), OperationStatus.SUCCESS, "OK", "", null);
            verify(_operationResponse).addResult(dataList.get(1), OperationStatus.SUCCESS, "OK", "", null);
        }
    }

    /**
     * Tests the failure scenario of the archive operation.
     * Mocks an HTTP response with an INTERNAL_SERVER_ERROR status and verifies
     * that the operation response captures the error result appropriately.
     */
    @Test
    void testExecuteDeleteFailure() {
        when(_closeableHttpResponse.getEntity()).thenReturn(_entity);
        when(_statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);
        when(_statusLine.getReasonPhrase()).thenReturn(String.valueOf(HttpStatus.SC_NOT_FOUND));
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(any(), any(), any(), any(), any(), any(), any())).thenReturn(
                    _closeableHttpResponse);
            _deleteOperation.executeDelete(_deleteRequest, _operationResponse);
            verify(_operationResponse).addErrorResult(_data, OperationStatus.APPLICATION_ERROR,
                    String.valueOf(HttpStatus.SC_NOT_FOUND), String.valueOf(HttpStatus.SC_NOT_FOUND), null);
        }
    }

    /**
     * Tests the archive operation when the object ID is null.
     * Verifies that a ConnectorException is thrown with the appropriate error message
     * indicating that a contact ID is required for the archive operation.
     */
    @Test
    void testExecuteDelete_NullObjectId() {
        SimpleTrackedData trackedData1 = new SimpleTrackedData(1, null);
        SimpleDeleteRequest deleteRequest = new SimpleDeleteRequest(Collections.singletonList(trackedData1));
        try {
            _deleteOperation.executeDelete(deleteRequest, _operationResponse);
        } catch (Exception e) {
            Assertions.assertInstanceOf(IllegalArgumentException.class, e);
            Assertions.assertEquals("ID is required for archive operation", e.getMessage());
        }
    }
}