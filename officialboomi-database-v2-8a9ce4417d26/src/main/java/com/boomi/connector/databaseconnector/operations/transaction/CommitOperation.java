// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;

/**
 * Commit operation
 */
public class CommitOperation extends TransactionOperation {

    /**
     * Constructor for CommitOperation
     *
     * @param connection
     */
    public CommitOperation(TransactionDatabaseConnectorConnection connection) {
        super(connection);
    }

    /**
     * Returns the transaction status
     *
     * @return
     */
    @Override
    protected String getTransactionStatus() {
        return TransactionConstants.TRANSACTION_COMMITTED;
    }

    /**
     * Executes the commit operation
     *
     * @param transactionCacheKey
     */
    @Override
    protected void executeTransaction(TransactionCacheKey transactionCacheKey) {
        getConnection().commitTransaction(transactionCacheKey);
    }
}
