//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import org.junit.Assert;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.junit.Before;
import org.junit.Test;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;
import com.boomi.connector.workdayprism.model.UploadMetadata;
import com.boomi.connector.workdayprism.model.UploadResponse;
import com.boomi.connector.workdayprism.operations.upload.UploadHelper;
import com.boomi.connector.workdayprism.utils.Constants;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * @author saurav.b.sengupta
 */
public class ImportOperationTest {

    private Logger logger = Mockito.mock(Logger.class);
    private OperationResponse operationResponse = Mockito.mock(OperationResponse.class);
    private ObjectData objectData = Mockito.mock(ObjectData.class, Mockito.RETURNS_DEEP_STUBS);
    private UpdateRequest updateRequest = Mockito.mock(UpdateRequest.class);
    private OperationContext opContext = Mockito.mock(OperationContext.class);
    private PrismOperationConnection connection = Mockito.mock(PrismOperationConnection.class,
            Mockito.RETURNS_DEEP_STUBS);
    private PrismResponse prismResponse = Mockito.mock(PrismResponse.class);
    private UploadResponse uploadResponse = Mockito.mock(UploadResponse.class);
    private PropertyMap opProps = Mockito.mock(PropertyMap.class);
    private UploadHelper uploadHelper;
    private CompleteBucketOperation completeBucketOperation;
    private UploadMetadata uploadMetadata;

    @Before
    public void init() throws IOException {
        Mockito.when(operationResponse.getLogger()).thenReturn(logger);
        Mockito.when(opContext.getObjectTypeId()).thenReturn("import");
        uploadHelper = new UploadHelper(connection);
        completeBucketOperation = new CompleteBucketOperation(connection);
        uploadMetadata = new UploadMetadata(inputDocument(), opProps, ArgumentMatchers.anyString());
    }

    @Test
    public void testImportOperationCall() {
        Assert.assertNotNull(new ImportOperation(connection).getConnection());
    }

    @Test
    public void testExecuteUpdate() throws IOException {
        Assert.assertNotNull(updateRequest);
        Mockito.when(updateRequest.iterator()).thenReturn(Collections.singletonList(objectData).iterator());
        Mockito.when(connection.createBucket(ArgumentMatchers.any(JsonNode.class))).thenReturn(prismResponse);
        //when(new UploadHelper(connection)).thenReturn(uploadHelper);
        uploadHelper.upload(inputDocument(), null);
        completeBucketOperation.processInput(inputDocument(), operationResponse, ArgumentMatchers.anyString());
        new ImportOperation(connection).execute(updateRequest, operationResponse);
        Assert.assertNotNull(operationResponse);
    }

    /**
     * Creates SimpleTrackedData
     *
     * @return SimpleTrackedData object
     */
    public static SimpleTrackedData inputDocument() {
        Map<String, String> dynamicProps = new HashMap<>(4);
        dynamicProps.put("bucket_id", "value1");
        dynamicProps.put(Constants.FIELD_FILENAME, "value2");
        dynamicProps.put(Constants.FIELD_BUCKET_ID, "value3");
        dynamicProps.put(Constants.FIELD_MAX_FILE_SIZE, "100000");

        String mockJsonData = "{\"test\":\"data\"}";
        ByteArrayInputStream inputStream = new ByteArrayInputStream(mockJsonData.getBytes(StandardCharsets.UTF_8));
        return new SimpleTrackedData(123, inputStream, null, dynamicProps);
    }
}
