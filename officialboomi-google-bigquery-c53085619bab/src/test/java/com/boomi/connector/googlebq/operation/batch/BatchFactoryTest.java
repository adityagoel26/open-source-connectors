// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.batch;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.util.StringUtil;

import org.junit.Test;
import org.restlet.data.Status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;

/**
 * @author Rohan Jain
 */
public class BatchFactoryTest {
    OperationResponse _response = mock(OperationResponse.class);


    public static BatchFactory createBatchFactory(List<ObjectData> inputDocs, int maxBatchCount, long maxSize,
                                                  OperationResponse response) {

        String templateSuffix = StringUtil.EMPTY_STRING;
        return new BatchFactory(mockUpdateRequest(inputDocs), templateSuffix, maxBatchCount, maxSize, response);
    }

    public static ObjectData createInputDocument(long documentSize) throws IOException {
        ObjectData document = mock(ObjectData.class);
        doReturn(documentSize).when(document).getDataSize();
        return document;
    }

    public static ObjectData createInputDocument(String dynamicTemplateSuffix, long documentSize) throws IOException {
        ObjectData document = mock(ObjectData.class);
        doReturn(documentSize).when(document).getDataSize();
        Map<String, String> dynamicProperties = mock(Map.class);
        doReturn(dynamicProperties).when(document).getDynamicProperties();
        doReturn(dynamicTemplateSuffix).when(dynamicProperties).get("templateSuffix");
        return document;
    }

    public static UpdateRequest mockUpdateRequest(List<ObjectData> inputDocs) {
        UpdateRequest updateRequest = mock(UpdateRequest.class);
        doReturn(inputDocs.iterator()).when(updateRequest).iterator();
        return updateRequest;
    }

    @Test
    public void testSingleBatchCreationWithSafeLimits() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 20, 2000, _response);
        verifyExpectedNumberOfBatches(factory, 1);
    }

    @Test
    public void testSingleBatchCreationWithLimitsExactMatch() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 2, 200, _response);
        verifyExpectedNumberOfBatches(factory, 1);

    }

    @Test
    public void testMultipleBatchesWithDynamicTemplateSuffix() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument("templateSuffix", 100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument("templateSuffix",100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 2, 2000, _response);
        verifyExpectedNumberOfBatches(factory, 2);
    }

    @Test
    public void testMultipleBatchesWithBatchCountExceeding() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 2, 2000, _response);
        verifyExpectedNumberOfBatches(factory, 2);
    }

    @Test
    public void testMultipleBatchesWithBatchSizeExceeding() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 20, 100, _response);
        verifyExpectedNumberOfBatches(factory, 4);
    }

    @Test
    public void testExceptionNotStoppingBatchCreation() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        //part of batch 1
        inputDocs.add(createInputDocument(100L));
        //exception occurs for below doc
        ObjectData errorDocument = createDocumentCausingIOException();
        inputDocs.add(errorDocument);
        //part of batch 1
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 20, 200, _response);
        verifyExpectedNumberOfBatches(factory, 2);
        verify(_response).addResult(eq(errorDocument), eq(OperationStatus.APPLICATION_ERROR), eq(String.valueOf
                        (Status.CLIENT_ERROR_BAD_REQUEST.getCode())), anyString(), any(Payload.class));
    }

    @Test
    public void testSingleDocumentExceedingSizeLimit() throws IOException {
        //this will cause a batch to be created with whatever size the
        //input document is. the api will return an error which will be thrown to user
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 1, 50, _response);
        verifyExpectedNumberOfBatches(factory, 1);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testRemoveThrowsException() throws IOException {
        List<ObjectData> inputDocs = new ArrayList<>();
        inputDocs.add(createInputDocument(100L));
        BatchFactory factory = createBatchFactory(inputDocs, 2, 200, _response);
        Iterator iterator = factory.iterator();
        iterator.remove();
    }

    private ObjectData createDocumentCausingIOException() throws IOException {
        ObjectData data = mock(ObjectData.class);
        doThrow(IOException.class).when(data).getDataSize();

        Logger logger = mock(Logger.class);
        doReturn(logger).when(data).getLogger();
        return data;
    }

    private void verifyExpectedNumberOfBatches(BatchFactory factory, int expectedNumberOfBatches) {
        Iterator iterator = factory.iterator();
        int numberOfBatches = 0;
        while (iterator.hasNext()) {
            numberOfBatches = numberOfBatches + 1;
            iterator.next();
        }
        assertEquals(numberOfBatches, expectedNumberOfBatches);
    }
}
