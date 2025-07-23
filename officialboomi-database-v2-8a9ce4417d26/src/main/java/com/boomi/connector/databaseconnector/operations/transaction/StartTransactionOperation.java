// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;

/**
 * Operation to start a transaction
 */
public class StartTransactionOperation extends TransactionOperation {

    /**
     * Constructor
     *
     * @param connection Connection to the database
     */
    public StartTransactionOperation(TransactionDatabaseConnectorConnection connection) {
        super(connection);
    }

    /**
     * Returns the transaction status
     *
     * @return Transaction status
     */
    @Override
    protected String getTransactionStatus() {
        return TransactionConstants.TRANSACTION_STARTED;
    }

    /**
     * Executes the start transaction operation
     *
     * @param transactionCacheKey
     */
    @Override
    protected void executeTransaction(TransactionCacheKey transactionCacheKey) {
        getConnection().startTransaction(transactionCacheKey);
    }
}
