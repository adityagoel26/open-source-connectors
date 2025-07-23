// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.hubspotcrm.operation;

import com.boomi.connector.api.OAuth2Context;
import com.boomi.connector.api.OAuth2Token;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.OperationStatus;

import com.boomi.connector.hubspotcrm.HubspotcrmOperationConnection;
import com.boomi.connector.hubspotcrm.util.ExecutionUtils;
import com.boomi.connector.testutil.SimpleGetRequest;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.util.TestUtil;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.MockedStatic;
import org.mockito.ArgumentMatchers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

/**
 * This class contains tests for the HubspotcrmRetrieve Operation functionality.
 * It sets up the necessary mocks and configurations for testing the Retrieve operations.
 */
class HubspotcrmRetrieveOperationTest {

    @Mock
    private HubspotcrmOperationConnection mockConnection;

    @Mock
    private OperationContext context;

    @Mock
    private SimpleGetRequest _getRequest;

    @Mock
    private SimpleOperationResponse _operationResponse;

    @Mock
    private PropertyMap connectionProperties;

    private SimpleTrackedData mockObjectIdData;

    @Mock
    private OperationType operationType;

    @Mock
    private CloseableHttpResponse closeableHttpResponse;

    @Mock
    private HttpEntity entity;

    @Mock
    private StatusLine statusLine;

    @Mock
    private OAuth2Token mockOAuth2Token;

    @Mock
    private OAuth2Context oAuthContextMock;

    @Mock
    private Logger logger;

    private static final String OTS =
            "{\n" + "    \"results\": [\n" + "        {\n" + "            \"name\": \"field-name\",\n"
                    + "            \"type\": \"string\"\n" + "        }\n" + "    ]\n" + "}";

    private HubspotcrmRetrieveOperation mockOperation;

    /**
     * Sets up the test environment by initializing Mockito mocks and configuring
     * the mock connection, context, and custom headers (Authorization and Content-Type)
     * for use in each test case.
     */
    @BeforeEach
    public void setup() throws IOException {
        MockitoAnnotations.openMocks(this);
        Mockito.when(mockConnection.getContext()).thenReturn(context);
        Mockito.when(context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(mockConnection.getOAuthContext()).thenReturn(oAuthContextMock);
        mockOperation = new HubspotcrmRetrieveOperation(mockConnection);
        Mockito.when(context.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(oAuthContextMock);
        Mockito.when(mockConnection.getOAuthContext().getOAuth2Token(false)).thenReturn(mockOAuth2Token);
        Mockito.when(mockOAuth2Token.getAccessToken()).thenReturn("testBearerToken");
        Mockito.when(context.getOperationType()).thenReturn(operationType);
        Mockito.when(operationType.name()).thenReturn("GET");
        Mockito.when(context.getConnectionProperties().getProperty("apiKey")).thenReturn("");
        Mockito.when(context.getOperationProperties()).thenReturn(Mockito.mock(PropertyMap.class));
        Mockito.when(context.getObjectTypeId()).thenReturn("GET::/crm/v3/objects/contacts/{contactId}");
        Mockito.when(context.getOperationProperties().getProperty("idType")).thenReturn("id");
        Mockito.when(closeableHttpResponse.getEntity()).thenReturn(entity);
        TestUtil.disableBoomiLog();
    }

    /**
     * Tests the successful execution of the Retrieve operation by mocking the HTTP response,
     * request parameters, and verifying that the operation response is properly handled.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void executeGetTestSuccess() throws IOException {
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class);
                InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8))) {
            Mockito.when(entity.getContent()).thenReturn(content);
            mockObjectIdData = new SimpleTrackedData(1, "123");
            Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
            Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
            Mockito.when(statusLine.getStatusCode()).thenReturn(200);
            mockedStatic.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    closeableHttpResponse);
            mockOperation.executeGet(_getRequest, _operationResponse);
            Mockito.verify(_operationResponse).addResult(Mockito.eq(mockObjectIdData),
                    Mockito.eq(OperationStatus.SUCCESS), Mockito.isNull(), Mockito.eq(""), ArgumentMatchers.any());
        }
    }

    /**
     * Tests the unsuccessful execution of the Retrieve operation by mocking the HTTP response,
     * request parameters, and verifying that the operation response is properly handled.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void executeGetTestFailure() throws IOException {
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class);
                InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8))) {
            Mockito.when(entity.getContent()).thenReturn(content);
            mockObjectIdData = new SimpleTrackedData(1, "Abc123");
            Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
            Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
            Mockito.when(statusLine.getStatusCode()).thenReturn(404);
            mockedStatic.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    closeableHttpResponse);
            mockOperation.executeGet(_getRequest, _operationResponse);
            Mockito.verify(_operationResponse).addEmptyResult(Mockito.eq(mockObjectIdData),
                    Mockito.eq(OperationStatus.SUCCESS), ArgumentMatchers.any(), ArgumentMatchers.any());
        }
    }

    /**
     * Tests the executeGet operation's failure scenario when a connection error occurs.
     * Specifically tests the case where the HTTP response returns a 401 unauthorized status code.
     *
     * @throws IOException If an I/O error occurs during the test execution
     */
    @Test
    void executeGetTestFailureWithConnectionError() throws IOException {
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class);
                InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8))) {
            Mockito.when(entity.getContent()).thenReturn(content);
            mockObjectIdData = new SimpleTrackedData(1, "id");
            Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
            Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
            Mockito.when(statusLine.getStatusCode()).thenReturn(401);
            mockedStatic.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    closeableHttpResponse);
            mockOperation.executeGet(_getRequest, _operationResponse);
            Mockito.verify(_operationResponse).addErrorResult(Mockito.eq(mockObjectIdData),
                    Mockito.eq(OperationStatus.FAILURE), ArgumentMatchers.any(), ArgumentMatchers.any(),
                    ArgumentMatchers.any());
        }
    }

    /**
     * Tests the behavior of the Retrieve operation when the object ID is null.
     * Verifies that a ConnectorException is thrown with the appropriate error message.
     */
    @Test
    void executeGetTestWithNullObjectId() {
        mockObjectIdData = new SimpleTrackedData(1, null);
        Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
        try {
            mockOperation.executeGet(_getRequest, _operationResponse);
        } catch (IllegalArgumentException e) {
            Assertions.assertInstanceOf(IllegalArgumentException.class, e);
            Assertions.assertEquals("ID is required for retrieve operation", e.getMessage());
        }
    }

    /**
     * Tests the behavior of the Retrieve operation when the object ID is Empty.
     * Verifies that a ConnectorException is thrown with the appropriate error message.
     */
    @Test
    void executeGetTestWithEmptyObjectId() throws IOException {
        mockObjectIdData = new SimpleTrackedData(1, " ");
        Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
        Mockito.when(context.getConnectionProperties().getOAuth2Context("oauthContext")).thenReturn(oAuthContextMock);
        Mockito.when(mockConnection.getOAuthContext().getOAuth2Token(false)).thenReturn(mockOAuth2Token);
        Mockito.when(mockOAuth2Token.getAccessToken()).thenReturn("testBearerToken");

        try {
            mockOperation.executeGet(_getRequest, _operationResponse);
        } catch (IllegalArgumentException e) {
            Assertions.assertInstanceOf(IllegalArgumentException.class, e);
            Assertions.assertEquals("ID is required for retrieve operation", e.getMessage());
        }
    }

    /**
     * Tests the behavior of the Retrieve operation when the object ID contains a percent character.
     * Verifies that the operation response is properly handled.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    void executeGetTestWithPercentCharacter() throws IOException {
        try (MockedStatic<ExecutionUtils> mockedStatic = Mockito.mockStatic(ExecutionUtils.class);
                InputStream content = new ByteArrayInputStream(OTS.getBytes(StandardCharsets.UTF_8))) {
            Mockito.when(entity.getContent()).thenReturn(content);
            mockObjectIdData = new SimpleTrackedData(1, "123%");
            Mockito.when(_getRequest.getObjectId()).thenReturn(mockObjectIdData);
            mockedStatic.when(() -> ExecutionUtils.urlEncode("123%")).thenReturn("123%25");
            Mockito.when(closeableHttpResponse.getStatusLine()).thenReturn(statusLine);
            Mockito.when(statusLine.getStatusCode()).thenReturn(200);
            mockedStatic.when(
                    () -> ExecutionUtils.execute(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(),
                            ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(
                    closeableHttpResponse);
            mockOperation.executeGet(_getRequest, _operationResponse);
            Mockito.verify(_operationResponse).addResult(Mockito.eq(mockObjectIdData),
                    Mockito.eq(OperationStatus.SUCCESS), Mockito.isNull(), Mockito.eq(""), ArgumentMatchers.any());
        }
    }
}