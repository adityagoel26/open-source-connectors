// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

public class ResponseHelperTest {

    @Test
    public void addSuccessTest() {
        TrackedData document = new SimpleTrackedData(0, null);
        Payload payload = Mockito.mock(Payload.class);
        SendMessageMetadata metadata = createSendMessageMetadata(payload);
        SendResult sendResult = SendResult.success(document, metadata);
        OperationResponse mockedResponse = Mockito.mock(OperationResponse.class);

        ResponseHelper.addSuccess(sendResult, mockedResponse);

        Mockito.verify(mockedResponse, Mockito.times(1)).addResult(document, OperationStatus.SUCCESS, "", null,
                payload);
    }

    @Test
    public void addSuccessesTest() {
        TrackedData document = new SimpleTrackedData(0, null);
        Payload payload = Mockito.mock(Payload.class);
        SendMessageMetadata metadata = createSendMessageMetadata(payload);
        SendResult sendResult = SendResult.success(document, metadata);
        OperationResponse mockedResponse = Mockito.mock(OperationResponse.class);

        ResponseHelper.addSuccesses(Collections.singleton(sendResult), mockedResponse);

        Mockito.verify(mockedResponse, Mockito.times(1)).addResult(document, OperationStatus.SUCCESS, "", null,
                payload);
    }

    @Test
    public void addErrorTest() {
        TrackedData document = new SimpleTrackedData(0, null);
        OperationResponse mockedResponse = Mockito.mock(OperationResponse.class);

        ResponseHelper.addError(document, "message", mockedResponse);

        Mockito.verify(mockedResponse, Mockito.times(1)).addResult(document, OperationStatus.APPLICATION_ERROR, "",
                "message", null);
    }

    private static SendMessageMetadata createSendMessageMetadata(Payload payload) {
        SendMessageMetadata metadata = Mockito.mock(SendMessageMetadata.class);
        Mockito.when(metadata.toPayload()).thenReturn(payload);

        return metadata;
    }
}
