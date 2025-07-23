// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GetResponseHandlerTest {

    private static final VerificationMode ONCE = Mockito.times(1);

    @Test
    public void addSuccessTest() {
        FilterData mockedDocument = mock(FilterData.class);
        OperationResponse mockedResponse = mock(OperationResponse.class);
        Payload mockedPayload = mock(Payload.class);
        GetResponseHandler responseHandler = new GetResponseHandler(mockedDocument, mockedResponse);

        responseHandler.addSuccess(mockedPayload);
        responseHandler.close();

        verify(mockedResponse, ONCE).addPartialResult(mockedDocument, OperationStatus.SUCCESS, "Success", null,
                mockedPayload);
        verify(mockedResponse, ONCE).finishPartialResult(mockedDocument);
    }

    @Test
    public void addSingleErrorTest() {
        FilterData mockedDocument = mock(FilterData.class, Mockito.RETURNS_DEEP_STUBS);
        OperationResponse mockedResponse = mock(OperationResponse.class);
        String expectedErrorMessage = "error message";
        Throwable throwable = new RuntimeException(expectedErrorMessage);
        GetResponseHandler responseHandler = new GetResponseHandler(mockedDocument, mockedResponse);

        responseHandler.addError(throwable);
        responseHandler.close();

        verify(mockedResponse, ONCE).addPartialResult(mockedDocument, OperationStatus.FAILURE, "Error", expectedErrorMessage, null);
        verify(mockedResponse, ONCE).finishPartialResult(mockedDocument);
    }

    @Test
    public void addEmptySuccessTest() {
        FilterData mockedDocument = mock(FilterData.class);
        OperationResponse mockedResponse = mock(OperationResponse.class);
        GetResponseHandler responseHandler = new GetResponseHandler(mockedDocument, mockedResponse);

        responseHandler.close();

        verify(mockedResponse, ONCE).addEmptyResult(mockedDocument, OperationStatus.SUCCESS, "Success", null);
    }

    @Test
    public void addErrorAfterSuccessTest() {
        FilterData mockedDocument = mock(FilterData.class, Mockito.RETURNS_DEEP_STUBS);
        OperationResponse mockedResponse = mock(OperationResponse.class);
        Payload mockedPayload = mock(Payload.class);
        String expectedErrorMessage = "error message";
        Throwable throwable = new RuntimeException(expectedErrorMessage);
        GetResponseHandler responseHandler = new GetResponseHandler(mockedDocument, mockedResponse);

        responseHandler.addSuccess(mockedPayload);
        responseHandler.addError(throwable);
        responseHandler.close();

        verify(mockedResponse, ONCE).addPartialResult(mockedDocument, OperationStatus.SUCCESS, "Success", null,
                mockedPayload);
        verify(mockedResponse, ONCE).addPartialResult(mockedDocument, OperationStatus.APPLICATION_ERROR, "Error", expectedErrorMessage, null);
        verify(mockedResponse, ONCE).finishPartialResult(mockedDocument);
    }

}
