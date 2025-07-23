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
 * This Class is used to test CommitTransactionOperation Class
 */
public class CommitTransactionOperationTest {

    private CommitOperation operation;
    private TransactionDatabaseConnectorConnection connection;

    @Before
    public void setup() {
        connection = Mockito.mock(TransactionDatabaseConnectorConnection.class);
        operation = new CommitOperation(connection);
    }

    /**
     * To test the constructor
     */
    @Test
    public void constructorShouldCreateObject(){
        CommitOperation commitOperation = new CommitOperation(connection);
        assertNotNull(commitOperation);
    }

    /**
     * To test the executeTransaction method
     */
    @Test
    public void executeTransactionCommitTransactionSuccessfully() {
        String              executionId         = "dummyExecutionId";
        TransactionCacheKey transactionCacheKey = new TransactionCacheKey(executionId, null);
        operation.executeTransaction(transactionCacheKey);
        verify(connection).commitTransaction(transactionCacheKey);
    }

}
