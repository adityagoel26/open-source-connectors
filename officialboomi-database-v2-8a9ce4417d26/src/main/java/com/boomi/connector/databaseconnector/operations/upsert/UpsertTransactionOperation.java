//Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;

import java.sql.Connection;
import java.util.Map;

/**
 * Upsert Operation capable of being part of a transaction.
 */
public class UpsertTransactionOperation extends UpsertOperation {

    /**
     * Instantiates a new upsert transaction operation.
     *
     * @param transactionConnection the connection
     */
    public UpsertTransactionOperation(TransactionDatabaseConnectorConnection transactionConnection) {
        super(transactionConnection);
    }

    /**
     * Returns the {@link TransactionDatabaseConnectorConnection} instance.
     *
     * @return
     */
    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

    /**
     * Overridden method to determine whether an upsert operation should be committed
     *
     * @return true when part of a transaction
     */
    @Override
    protected boolean shouldCommit() {
        return true;
    }

    /**
     * Executes a size-limited upsert operation on the database.
     *
     * @param request the {@link UpdateRequest} containing the data to be used in the upsert operation
     * @param response the {@link OperationResponse} to be populated with the results of the upsert operation
     */
    @Override
    public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {

        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = getConnection();

        //create a cache key for transaction
        TransactionCacheKey transactionCacheKey = transactionDatabaseConnectorConnection.createCacheKey(
                request.getTopLevelExecutionId());
        //get active connection object from connector cache
        Connection sqlConnection = transactionDatabaseConnectorConnection.getDatabaseConnection(transactionCacheKey);
        CustomResponseUtil.logJoinTransactionStatus(response.getLogger(), transactionCacheKey.toString());

        String schemaNameFromConnection = transactionDatabaseConnectorConnection.getSchemaName();
        Long readTimeOut = transactionDatabaseConnectorConnection.getReadTimeOut();

        Map<String, String> properties = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);

        //log the properties in process logs
        CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());

        //create metadata from response
        PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, properties);

        executeSizeLimitedUpdateForUpsert(request, response, sqlConnection, schemaNameFromConnection,
                readTimeOut, payloadMetadata);
    }
}
