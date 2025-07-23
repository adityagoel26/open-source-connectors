//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.workdayprism.PrismOperationConnection;
import com.boomi.connector.workdayprism.model.PrismResponse;

import org.junit.Assert;
import org.junit.Test;
import java.io.IOException;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


/**
 * @author saurav.b.sengupta
 *
 */
public class GetOperationTest {

    private static final String NOT_FOUND = "404";
    private static final String BUCKET_ID = "bucketId";

    private OperationResponse operationResponse = Mockito.mock(OperationResponse.class);
    private ObjectIdData objectIdData = Mockito.mock(ObjectIdData.class, Mockito.RETURNS_DEEP_STUBS);
    private GetRequest getRequest = Mockito.mock(GetRequest.class);
    private PrismOperationConnection connection = Mockito.mock(PrismOperationConnection.class, Mockito.RETURNS_DEEP_STUBS);
    private PrismResponse prismResponse = Mockito.mock(PrismResponse.class);

    @Test
    public void testGetConnectionCall() {
        Assert.assertNotNull(new GetOperation(connection).getConnection());
    }

    @Test
    public void testAdditionOfApplicationErorWhenGetObjectReturnsNotFound() throws IOException {
        Assert.assertNotNull(getRequest);
        Mockito.when(getRequest.getObjectId()).thenReturn(objectIdData);
        Mockito.when(objectIdData.getObjectId()).thenReturn(BUCKET_ID);
        Mockito.when(connection.getBucket(ArgumentMatchers.eq(BUCKET_ID))).thenReturn(prismResponse);
        Mockito.when(prismResponse.isNotFound()).thenReturn(true);

        new GetOperation(connection).executeGet(getRequest, operationResponse);
    } 

    @Test
    public void testAdditionOfApplicationErrorWhenEmptyBucketIdEntered() throws IOException {
        Mockito.when(getRequest.getObjectId()).thenReturn(objectIdData);
        Mockito.when(objectIdData.getObjectId()).thenReturn(null);

        new GetOperation(connection).executeGet(getRequest, operationResponse);

        Mockito.verify(operationResponse).addResult(ArgumentMatchers.eq(objectIdData), ArgumentMatchers.eq(OperationStatus.APPLICATION_ERROR), ArgumentMatchers.eq(""),
                ArgumentMatchers.eq("the ID parameter is empty or only contains blank spaces"), ArgumentMatchers.isNull(Payload.class));
    }

    @Test
    public void testAdditionOfApplicationErrorWhenAConnectorExceptionIsThrown() throws IOException {
        Mockito.when(getRequest.getObjectId()).thenReturn(objectIdData);
        Mockito.when(objectIdData.getObjectId()).thenReturn(BUCKET_ID);
        Mockito.doThrow(new ConnectorException("")).when(connection).getBucket(ArgumentMatchers.eq(BUCKET_ID));

        new GetOperation(connection).executeGet(getRequest, operationResponse);

        Mockito.verify(operationResponse).addResult(ArgumentMatchers.eq(objectIdData), ArgumentMatchers.eq(OperationStatus.APPLICATION_ERROR), ArgumentMatchers.eq(""),
                ArgumentMatchers.eq("Unknown failure"), ArgumentMatchers.isNull(Payload.class));
    }

    @Test
    public void testFailureWhenExceptionIsThrown() throws IOException {
        Mockito.when(getRequest.getObjectId()).thenReturn(objectIdData);
        Mockito.when(objectIdData.getObjectId()).thenReturn(BUCKET_ID);
        Exception exception = new NullPointerException();
        Mockito.doThrow(exception).when(connection).getBucket(ArgumentMatchers.eq(BUCKET_ID));

        new GetOperation(connection).executeGet(getRequest, operationResponse);

        Mockito.verify(operationResponse).addErrorResult(ArgumentMatchers.eq(objectIdData), ArgumentMatchers.eq(OperationStatus.FAILURE), ArgumentMatchers.eq(""),
                ArgumentMatchers.eq("java.lang.NullPointerException"), ArgumentMatchers.eq(exception));
    }
}
