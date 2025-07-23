// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.util.StringUtil;
import com.boomi.util.TestUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

class HubspotcrmUpdateOperationTest {

    @Mock
    private HubspotcrmOperationConnection mockConnection;

    private UpdateRequest updateRequest;

    @Mock
    private OperationResponse mockOperationResponse;

    @Mock
    private HubspotcrmUpdateOperation hubspotcrmUpdateOperation;

    @Mock
    private OperationType operationType;

    @Mock
    private CloseableHttpResponse closeableHttpResponse;

    @Mock
    private OAuth2Context oAuthContextMock;

    @Mock
    private StatusLine statusLine;

    @Mock
    private HttpEntity mockEntity;

    @Mock
    private OperationContext context;
    @Mock
    private PropertyMap connectionProperties;

    @Mock
    private Logger mockLogger;

    private SimpleTrackedData data;

    /**
     * Set up the test environment by initializing mocks and creating an instance of
     * {@code HubspotcrmUpdateOperation} before each test case.
     * <p>
     * This ensures a clean test environment with proper mock initialization for every test.
     * </p>
     */
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        Map<String, String> customHeaders = new HashMap<>();
        customHeaders.put("Authorization", "Bearer someToken");
        customHeaders.put("Content-Type", "application/json");
        Mockito.when(context.getOperationProperties()).thenReturn(connectionProperties);
        Mockito.when(context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(connectionProperties.getProperty("url")).thenReturn("https://api.hubapi.com");
        Mockito.when(connectionProperties.getCustomProperties("customHeaders")).thenReturn(customHeaders);
        Mockito.when(context.getObjectTypeId()).thenReturn("PATCH::/crm/v3/objects/contacts");
        Mockito.when(context.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(oAuthContextMock);
        Mockito.when(context.getOperationType()).thenReturn(operationType);
        Mockito.when(operationType.name()).thenReturn("DELETE");
        Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(mockConnection.getContext()).thenReturn(context);
        String objectId = "1234567";
        HashMap<String, Object> properties = new HashMap<>();
        properties.put("id", objectId);
        String mockJsonData = "{\"test\":\"data\"}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockJsonData.getBytes(StandardCharsets.UTF_8));
        MutableDynamicPropertyMap mp = new MutableDynamicPropertyMap(properties);
        data = new SimpleTrackedData(1, inputStream, Collections.emptyMap(), Collections.emptyMap(), mp);
        updateRequest = new SimpleUpdateRequest(Collections.singletonList(data));
        // Setup Logger
        Mockito.doNothing().when(mockLogger).log(Mockito.any(Level.class), Mockito.anyString(),
                Mockito.any(Object.class));
        TestUtil.disableBoomiLog();
        hubspotcrmUpdateOperation = new HubspotcrmUpdateOperation(mockConnection);
    }

    /**
     * Tests the executeUpdate method of HubspotUpdateOperation to verify successful updation of records.
     * <p>
     * This test verifies that:
     * - The operation correctly processes input data from ObjectData
     * - The HTTP request is executed with proper parameters
     * - A successful creation (200 Created) response is handled appropriately
     *
     * @throws IOException if there's an error processing the request or response
     */

    @Test
    void testExecuteUpdate_Success() throws IOException {
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
        Mockito.when(statusLine.getReasonPhrase()).thenReturn("OK");
        Mockito.when(closeableHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(closeableHttpResponse);
            hubspotcrmUpdateOperation.executeUpdate(updateRequest, mockOperationResponse);
            Mockito.verify(mockOperationResponse).addResult(Mockito.eq(data), Mockito.eq(OperationStatus.SUCCESS),
                    Mockito.eq("200"), Mockito.eq("OK"), Mockito.any());
            Assert.assertEquals(HttpStatus.SC_OK, closeableHttpResponse.getStatusLine().getStatusCode());
        }
    }

    /**
     * Tests the executeUpdate method of HubspotUpdateOperation for failure scenarios.
     * <p>
     * This test verifies that:
     * - The operation correctly handles HTTP 400 Bad Request responses
     * - Error states are properly propagated through the operation response
     * - The operation maintains expected behavior even during failure conditions
     *
     * @throws IOException if there's an error processing the request or response
     */
    @Test
    void testExecuteUpdate_Failure() throws IOException {
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_BAD_REQUEST);
        Mockito.when(statusLine.getReasonPhrase()).thenReturn("Bad Request");
        Mockito.when(closeableHttpResponse.getEntity()).thenReturn(mockEntity);
        Mockito.when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream("{}".getBytes()));
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class)) {
            mockedStatic.when(() -> ExecutionUtils.execute(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(),
                    Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(closeableHttpResponse);
            hubspotcrmUpdateOperation.executeUpdate(updateRequest, mockOperationResponse);
            Mockito.verify(mockOperationResponse).addResult(Mockito.eq(data),
                    Mockito.eq(OperationStatus.APPLICATION_ERROR),
                    Mockito.eq(StringUtil.toString(HttpStatus.SC_BAD_REQUEST)), Mockito.eq("Bad Request"),
                    Mockito.any());
        }
    }
}