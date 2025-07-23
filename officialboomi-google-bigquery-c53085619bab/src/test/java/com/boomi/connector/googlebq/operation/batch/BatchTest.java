// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.batch;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.util.StringUtil;

import org.junit.Test;
import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Rohan Jain
 */
public class BatchTest {

    private final OperationResponse _response = mock(OperationResponse.class);
    private final Batch _batch = new Batch(StringUtil.EMPTY_STRING, 5, 400, _response);

    @Test
    public void testAddDocument() throws IOException {
        long documentSize = 200;
        ObjectData inputDocument = BatchFactoryTest.createInputDocument(documentSize);
        _batch.addDocument(inputDocument);

        assertEquals(1, _batch.getBatchCount());
        assertEquals( documentSize, _batch.getSizeInBytes());

        for (ObjectData documentInBatch : _batch) {
            assertEquals(documentInBatch, inputDocument);
        }
    }

    @Test
    public void testAddCombinedApplicationError() throws IOException {
        Status errorStatus = Status.CLIENT_ERROR_UNAUTHORIZED;
        Response response = getMockErrorResponse(errorStatus);
        ObjectData inputDocument = BatchFactoryTest.createInputDocument(200);
        _batch.addDocument(inputDocument);
        _batch.addCombinedErrorResult(response, null, OperationStatus.APPLICATION_ERROR);

        verify(_response).addCombinedResult(anyCollection(), eq(OperationStatus.APPLICATION_ERROR), eq(Integer
                .toString(errorStatus.getCode())), eq(errorStatus.getDescription()), any(Payload.class));
    }

    private Response getMockErrorResponse(Status errorStatus) {
        Response response = mock(Response.class);
        doReturn(errorStatus).when(response).getStatus();
        return response;
    }
}
