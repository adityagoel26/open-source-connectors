// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.transaction;

import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;

/**
 * Rollback operation
 */
public class RollbackOperation extends TransactionOperation {

    /**
     * Constructor for Rollback Operation
     *
     * @param connection
     */
    public RollbackOperation(TransactionDatabaseConnectorConnection connection) {
        super(connection);
    }

    /**
     * Returns the transaction status
     *
     * @return Transaction status
     */
    @Override
    protected String getTransactionStatus() {
        return TransactionConstants.TRANSACTION_ROLLED_BACK;
    }

    /**
     * Executes the rollback operation
     *
     * @param transactionCacheKey
     */
    @Override
    protected void executeTransaction(TransactionCacheKey transactionCacheKey) {
        getConnection().rollbackTransaction(transactionCacheKey);
    }
}
