// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.util.TestUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HubspotcrmCreateOperationTest {

    @Mock
    private HubspotcrmOperationConnection _connection;

    @Mock
    private UpdateRequest mockUpdateRequest;

    @Mock
    private OperationResponse mockOperationResponse;

    @Mock
    private ObjectData mockObjectData;

    private HubspotcrmCreateOperation hubspotCreateOperation;

    @Mock
    private OperationContext _context;

    @Mock
    private PropertyMap _connectionProperties;

    @Mock
    private OAuth2Context oAuthContextMock;

    @Mock
    private OperationType _operationType;

    @Mock
    private CloseableHttpResponse _closeableHttpResponse;

    @Mock
    private StatusLine _statusLine;
    @Mock
    private Logger mockLogger;

    @Mock
    private HttpEntity mockEntity;

    /**
     * Set up the test environment by initializing mocks and creating an instance of
     * {@code StripeCreateOperation} before each test case.
     * <p>
     * This ensures a clean test environment with proper mock initialization for every test.
     * </p>
     */
    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        mockObjectData = Mockito.mock(ObjectData.class);
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Authorization", "Bearer someToken");
        customHeaders.put("Content-Type", "application/json");

        Mockito.when(_context.getOperationProperties()).thenReturn(_connectionProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(_connectionProperties);
        Mockito.when(_connectionProperties.getProperty("idType")).thenReturn("emailId");
        Mockito.when(_connectionProperties.getProperty("url")).thenReturn("https://api.hubapi.com");
        Mockito.when(_connectionProperties.getCustomProperties("customHeaders")).thenReturn(customHeaders);
        Mockito.when(_context.getObjectTypeId()).thenReturn("POST::/crm/v3/objects/contacts");
        Mockito.when(_context.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(oAuthContextMock);
        Mockito.when(_context.getOperationType()).thenReturn(_operationType);
        Mockito.when(_operationType.name()).thenReturn("DELETE");
        Mockito.when(_closeableHttpResponse.getStatusLine()).thenReturn(_statusLine);
        Mockito.when(_connection.getContext()).thenReturn(_context);
        // Setup Logger
        Mockito.when(mockObjectData.getLogger()).thenReturn(mockLogger);
        Mockito.doNothing().when(mockLogger).log(Mockito.any(Level.class), Mockito.anyString(),
                Mockito.any(Object.class));
        TestUtil.disableBoomiLog();
        hubspotCreateOperation = new HubspotcrmCreateOperation(_connection);
    }

    /**
     * Tests the executeUpdate method of HubspotCreateOperation to verify successful creation of records.
     * <p>
     * This test verifies that:
     * - The operation correctly processes input data from ObjectData
     * - The HTTP request is executed with proper parameters
     * - A successful creation (201 Created) response is handled appropriately
     *
     * @throws IOException if there's an error processing the request or response
     */
    @Test
    public void testExecuteUpdate_ShouldProcessEachObjectData() throws IOException {
        // Mock the UpdateRequest to return an iterator over ObjectData
        Iterator<ObjectData> mockIterator = Collections.singletonList(mockObjectData).iterator();
        Mockito.when(mockUpdateRequest.iterator()).thenReturn(mockIterator);

        // Mock the behavior of getData() in ObjectData
        String mockJsonData = "{\"properties\": {\"email\": \"test@example.com\", \"firstname\": \"Test\"}}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockJsonData.getBytes(StandardCharsets.UTF_8));

        Mockito.when(mockObjectData.getData()).thenReturn(inputStream);

        Mockito.when(_statusLine.getStatusCode()).thenReturn(HttpStatus.SC_CREATED);
        Mockito.when(_statusLine.getReasonPhrase()).thenReturn("OK");
        Mockito.when(_closeableHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(_closeableHttpResponse);
            hubspotCreateOperation.executeUpdate(mockUpdateRequest, mockOperationResponse);
            Mockito.verify(mockOperationResponse).addResult(Mockito.eq(mockObjectData),
                    Mockito.eq(OperationStatus.SUCCESS), Mockito.eq("201"), Mockito.eq("OK"), Mockito.any());
            Assert.assertEquals(HttpStatus.SC_CREATED, _closeableHttpResponse.getStatusLine().getStatusCode());
        }
    }

    /**
     * Tests the executeUpdate method of HubspotCreateOperation for failure scenarios.
     * <p>
     * This test verifies that:
     * - The operation correctly handles HTTP 400 Bad Request responses
     * - Error states are properly propagated through the operation response
     * - The operation maintains expected behavior even during failure conditions
     *
     * @throws IOException if there's an error processing the request or response
     */
    @Test
    public void testExecuteUpdate_Failure() throws IOException {
        // Mock the UpdateRequest to return an iterator over ObjectData
        Iterator<ObjectData> mockIterator = Collections.singletonList(mockObjectData).iterator();
        Mockito.when(mockUpdateRequest.iterator()).thenReturn(mockIterator);

        // Mock the behavior of getData() in ObjectData
        String mockJsonData = "{\"properties\": {\"email\": \"test@example.com\", \"firstname\": \"Test\"}}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockJsonData.getBytes(StandardCharsets.UTF_8));

        Mockito.when(mockObjectData.getData()).thenReturn(inputStream);

        Mockito.when(_statusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);
        Mockito.when(_statusLine.getReasonPhrase()).thenReturn("Bad Request");
        Mockito.when(_closeableHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));

        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(_closeableHttpResponse);
            hubspotCreateOperation.executeUpdate(mockUpdateRequest, mockOperationResponse);
            Mockito.verify(mockOperationResponse).addResult(Mockito.eq(mockObjectData),
                    Mockito.eq(OperationStatus.APPLICATION_ERROR), Mockito.eq("400"), Mockito.eq("Bad Request"),
                    ArgumentMatchers.any());
        }
    }
}