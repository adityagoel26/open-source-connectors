//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.util.io.FastByteArrayInputStream;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import org.junit.Assert;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class CompleteBucketOperationTest {
    private static final String PROPERTY_WAIT_FOR_COMPLETION = "wait_for_completion";
    private static final String PROPERTY_TIMEOUT = "timeout";
    private static final String BUCKET_ID = "1234";
    private static final String INPUT_JSON = "{\"bucket_id\":\"" + BUCKET_ID + "\"}";


    private OperationResponse operationResponse = Mockito.mock(OperationResponse.class);
    private ObjectData objectData = Mockito.mock(ObjectData.class, Mockito.RETURNS_DEEP_STUBS);
    private UpdateRequest updateRequest = Mockito.mock(UpdateRequest.class);
    private PrismOperationConnection connection = Mockito.mock(PrismOperationConnection.class, Mockito.RETURNS_DEEP_STUBS);
    private PrismResponse prismResponse = Mockito.mock(PrismResponse.class);

    private JsonNode buildStatusResponse(String condition) {
        ObjectNode json = JSONUtil.newObjectNode();
        json.putObject("state").put("descriptor", condition);
        return json;
    }

    @Test
    public void shouldCallConnection() {
        Assert.assertNotNull(new GetOperation(connection).getConnection());
    }

    @Test
    public void shouldAddApplicationErrorWhenInvalidInput() throws IOException {
        Mockito.when(connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION)).thenReturn(false);
        Mockito.when(connection.getLongProperty(PROPERTY_TIMEOUT, 0L)).thenReturn(0L);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);

        Mockito.verify(connection).getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION);
        Mockito.verify(connection).getLongProperty(PROPERTY_TIMEOUT, 0L);
        Mockito.verify(operationResponse).addResult(ArgumentMatchers.eq(objectData), ArgumentMatchers.eq(OperationStatus.APPLICATION_ERROR), ArgumentMatchers.eq(""),
                ArgumentMatchers.anyString(), ArgumentMatchers.isNull(Payload.class));
    }

    @Test
    public void shouldAddResultWithoutWaitingForCompletion() throws IOException {
        Assert.assertNotNull(updateRequest);
        Mockito.when(connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION)).thenReturn(false);
        Mockito.when(connection.getLongProperty(PROPERTY_TIMEOUT, 0L)).thenReturn(0L);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Mockito.when(objectData.getData()).thenReturn(new FastByteArrayInputStream(INPUT_JSON.getBytes()));
        Mockito.when(connection.completeBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(prismResponse.isSuccess()).thenReturn(true);

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);
    }

    @Test
    public void shouldNotWaitForCompletionWhenUnsuccessfulResponse() throws IOException {
    	Assert.assertNotNull(updateRequest);
        Mockito.when(connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION)).thenReturn(true);
        Mockito.when(connection.getLongProperty(PROPERTY_TIMEOUT, 0L)).thenReturn(0L);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Mockito.when(objectData.getData()).thenReturn(new FastByteArrayInputStream(INPUT_JSON.getBytes()));
        Mockito.when(connection.completeBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(prismResponse.isSuccess()).thenReturn(false);

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);
    }

    @Test
    public void shouldWaitForCompletion() throws IOException {
    	Assert.assertNotNull(updateRequest);
        Mockito.when(connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION)).thenReturn(true);
        Mockito.when(connection.getLongProperty(PROPERTY_TIMEOUT, 0L)).thenReturn(0L);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Mockito.when(objectData.getData()).thenReturn(new FastByteArrayInputStream(INPUT_JSON.getBytes()));
        Mockito.when(connection.completeBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(connection.getBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(prismResponse.isSuccess()).thenReturn(true);
        Mockito.when(prismResponse.getJsonEntity()).thenReturn(buildStatusResponse("SUCCESS"));

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);
    }

    @Test
    public void shouldWaitForCompletionAndGetFailedCondition() throws IOException {
    	Assert.assertNotNull(updateRequest);
        Mockito.when(connection.getBooleanProperty(PROPERTY_WAIT_FOR_COMPLETION)).thenReturn(true);
        Mockito.when(connection.getLongProperty(PROPERTY_TIMEOUT, 0L)).thenReturn(0L);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Mockito.when(objectData.getData()).thenReturn(new FastByteArrayInputStream(INPUT_JSON.getBytes()));
        Mockito.when(connection.completeBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(connection.getBucket(BUCKET_ID)).thenReturn(prismResponse);
        Mockito.when(prismResponse.isSuccess()).thenReturn(true);
        Mockito.when(prismResponse.getJsonEntity()).thenReturn(buildStatusResponse("FAILED"));

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);

    }

    @Test
    public void shouldFailureWhenExceptionIsThrown() throws IOException {
    	Assert.assertNotNull(updateRequest);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());

        Exception exception = new NullPointerException();
        Mockito.doThrow(exception).when(objectData).getData();

        new CompleteBucketOperation(connection).executeUpdate(updateRequest, operationResponse);

    }
}
