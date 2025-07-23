// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;

/**
 * This Class is used to test RollbackTransactionOperation Class
 */
public class RollbackTransactionOperationTest {

    private RollbackOperation operation;
    private TransactionDatabaseConnectorConnection connection;

    @Before
    public void setup() {
        connection = Mockito.mock(TransactionDatabaseConnectorConnection.class);
        operation = new RollbackOperation(connection);
    }

    /**
     * To test the constructor
     */
    @Test
    public void constructorShouldCreateObject(){
        StartTransactionOperation startTransactionOperation = new StartTransactionOperation(connection);
        assertNotNull(startTransactionOperation);
    }

    /**
     * To test the executeTransaction method
     */
    @Test
    public void executeTransactionRollbackTransactionSuccessfully() {
        String              executionId         = "dummyExecutionId";
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(executionId, null);
        operation.executeTransaction(transactionCacheKey);
        verify(connection).rollbackTransaction(transactionCacheKey);
    }

}
