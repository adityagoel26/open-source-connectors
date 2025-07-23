// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;

import static org.mockito.Mockito.verify;

/**
 * Test class for {@link TransactionOperation}
 */
public class TransactionOperationTest {

    private static final String TEST_JSON =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    private static final String EXECUTION_ID = "executionId";
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(TEST_JSON);
    private final SimpleOperationResponse _simpleOperationResponse = ResponseUtil.getResponse(
            Collections.singleton(_document));
    private final TransactionDatabaseConnectorConnection _trnscDbConnection = Mockito.mock(
            TransactionDatabaseConnectorConnection.class);
    private TransactionOperation operation;

    @Before
    public void setup() {
        operation = new StartTransactionOperation(_trnscDbConnection);
    }

    /**
     * To test the executeUpdate method
     */
    @Test
    public void executeUpdateTransactionSuccess() {
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(EXECUTION_ID, null);

        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(transactionCacheKey);
        operation.executeUpdate(ResponseUtil.toRequest(Collections.singleton(
                _document)), _simpleOperationResponse);
        verify(_trnscDbConnection).startTransaction(transactionCacheKey);
    }

    /**
     * To test the executeUpdate method
     */
    @Test
    public void executeUpdateTransactionFail() {
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(EXECUTION_ID, null);

        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(transactionCacheKey);
        Mockito.doThrow(new ConnectorException("DummyStatusCode", "Dummy Exception!")).when(_trnscDbConnection).startTransaction(transactionCacheKey);
        operation.executeUpdate(ResponseUtil.toRequest(Collections.singleton(
                _document)), _simpleOperationResponse);
        Assert.assertEquals("Dummy Exception!", _simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertEquals("DummyStatusCode", _simpleOperationResponse.getResults().get(0).getStatusCode());
    }
}
