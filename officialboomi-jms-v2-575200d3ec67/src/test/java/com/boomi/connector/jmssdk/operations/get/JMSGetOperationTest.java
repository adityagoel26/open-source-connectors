// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSReceiver;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.get.strategy.ReceiveMode;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.doubles.BytesMessageDouble;
import com.boomi.util.CollectionUtil;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.jms.Message;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(AdapterPoolManager.class)
@Ignore("Temporary disabling tests using PowerMock")
public class JMSGetOperationTest {

    @Test
    public void emptySuccessTest() {
        JMSOperationConnection mockedConnection = mockConnection();
        JMSReceiver mockedReceiver = mock(JMSReceiver.class, Mockito.RETURNS_MOCKS);
        mockAdapterPoolManager(null, mockedReceiver);

        SimpleTrackedData document = new SimpleTrackedData(1, null);
        QueryRequest mockedRequest = mockRequest(document);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));
        JMSGetOperation operation = new JMSGetOperation(mockedConnection);

        operation.executeQuery(mockedRequest, response);

        assertResultsQuantity(1, response);
        SimpleOperationResult result = getResult(response);
        assertEmptySuccess(result);
    }

    private static void assertEmptySuccess(SimpleOperationResult result) {
        assertThat(OperationStatus.SUCCESS, is(result.getStatus()));
        assertTrue("it should not have any payloads", result.getPayloads().isEmpty());
    }

    @Test
    public void receiveNoWaitTest() {
        byte[] payload = "the payload".getBytes(StandardCharsets.UTF_8);
        String expectedDestination = "the destination";
        Message message = new BytesMessageDouble(expectedDestination, payload);
        JMSOperationConnection mockedConnection = mockConnection();
        JMSReceiver mockedReceiver = mock(JMSReceiver.class, Mockito.RETURNS_MOCKS);
        mockAdapterPoolManager(message, mockedReceiver);

        SimpleTrackedData document = new SimpleTrackedData(1, null);
        QueryRequest mockedRequest = mockRequest(document);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));
        JMSGetOperation operation = new JMSGetOperation(mockedConnection);

        operation.executeQuery(mockedRequest, response);

        assertResultsQuantity(1, response);
        SimpleOperationResult result = getResult(response);
        assertThat(OperationStatus.SUCCESS, is(result.getStatus()));
        assertPayload(payload, result);

        Map<String, String> trackedProps = CollectionUtil.getFirst(result.getPayloadMetadatas()).getTrackedProps();
        assertThat(trackedProps.get("destination"), is(expectedDestination));
        assertThat(trackedProps.get("message_type"), is("BYTE_MESSAGE"));
    }

    @Test
    public void failedCommitShouldAddAppErrorWithPayloadTest() {
        byte[] payload = "the payload".getBytes(StandardCharsets.UTF_8);
        String expectedDestination = "the destination";
        Message message = new BytesMessageDouble(expectedDestination, payload);

        // set the receiver to throw an error when invoking commit
        JMSReceiver mockedReceiver = mock(JMSReceiver.class, Mockito.RETURNS_MOCKS);
        doThrow(ConnectorException.class).when(mockedReceiver).commit();

        JMSOperationConnection mockedConnection = mockConnection();
        mockAdapterPoolManager(message, mockedReceiver);

        SimpleTrackedData document = new SimpleTrackedData(1, null);
        QueryRequest mockedRequest = mockRequest(document);
        SimpleOperationResponse response = ResponseUtil.getResponse(Collections.singleton(document));
        JMSGetOperation operation = new JMSGetOperation(mockedConnection);

        operation.executeQuery(mockedRequest, response);

        assertResultsQuantity(1, response);
        SimpleOperationResult result = getResult(response);
        assertThat(OperationStatus.APPLICATION_ERROR, is(result.getStatus()));
        assertPayload(payload, result);

        Map<String, String> trackedProps = CollectionUtil.getFirst(result.getPayloadMetadatas()).getTrackedProps();
        assertThat(trackedProps.get("destination"), is(expectedDestination));
        assertThat(trackedProps.get("message_type"), is("BYTE_MESSAGE"));
    }

    private static void assertPayload(byte[] expected, SimpleOperationResult actualResult) {
        List<byte[]> payloads = actualResult.getPayloads();
        assertThat(1, is(payloads.size()));
        byte[] actualPayload = CollectionUtil.getFirst(payloads);
        assertThat(expected, is(actualPayload));
    }

    private static void assertResultsQuantity(int expectedQuantity, SimpleOperationResponse response) {
        assertThat(expectedQuantity, is(response.getResults().size()));
    }

    private SimpleOperationResult getResult(SimpleOperationResponse response) {
        return CollectionUtil.getFirst(response.getResults());
    }

    private static JMSOperationConnection mockConnection() {
        JMSOperationConnection mockedConnection = mock(JMSOperationConnection.class, Mockito.RETURNS_DEEP_STUBS);
        when(mockedConnection.getReceiveMode()).thenReturn(ReceiveMode.NO_WAIT);
        when(mockedConnection.getOperationContext().createMetadata()).thenReturn(new SimplePayloadMetadata());
        return mockedConnection;
    }

    private static void mockAdapterPoolManager(Message message, JMSReceiver mockedReceiver) {
        AdapterPool mockedPool = mock(AdapterPool.class);
        GenericJndiBaseAdapter mockedAdapter = mock(GenericJndiBaseAdapter.class);

        PowerMockito.mockStatic(AdapterPoolManager.class);
        PowerMockito.when(AdapterPoolManager.getPool(Mockito.any())).thenReturn(mockedPool);
        when(mockedPool.createAdapter()).thenReturn(mockedAdapter);
        when(mockedAdapter.createReceiver(anyObject(), anyString(), anyInt())).thenReturn(mockedReceiver);
        when(mockedAdapter.getDestinationType(message)).thenReturn(DestinationType.BYTE_MESSAGE);
        when(mockedReceiver.receiveNoWait()).thenReturn(message);
    }

    private static QueryRequest mockRequest(FilterData document) {
        QueryRequest mockedRequest = mock(QueryRequest.class);
        when(mockedRequest.getFilter()).thenReturn(document);
        return mockedRequest;
    }
}
