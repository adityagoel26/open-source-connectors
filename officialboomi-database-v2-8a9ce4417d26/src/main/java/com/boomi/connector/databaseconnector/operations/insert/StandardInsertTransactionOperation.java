// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;

import java.sql.Connection;
import java.util.Map;

/**
 * Standard Insert Operation capable of being part of a transaction.
 */
public class StandardInsertTransactionOperation extends StandardInsertOperation{

    /**
     * Instantiates a new standard insert transaction operation.
     *
     * @param transactionConnection the connection
     */
    public StandardInsertTransactionOperation(TransactionDatabaseConnectorConnection transactionConnection) {
        super(transactionConnection);
    }

    /**
     * Returns the {@link TransactionDatabaseConnectorConnection} instance.
     * @return TransactionDatabaseConnectorConnection connection
     */
    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

    /**
     * Execute size limited update.
     *
     * @param request  the request
     * @param response the response
     */
    @Override
    protected void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = getConnection();
        //creating a cache key for the transaction
        TransactionCacheKey transactionCacheKey = transactionDatabaseConnectorConnection.createCacheKey(
                request.getTopLevelExecutionId());
        // Getting the database connection from the cache
        Connection sqlConnection = transactionDatabaseConnectorConnection.getDatabaseConnection(transactionCacheKey);
        CustomResponseUtil.logJoinTransactionStatus(response.getLogger(), transactionCacheKey.toString());

        String query = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.QUERY, "");
        String schemaName = (String) getContext().getOperationProperties()
                .get(DatabaseConnectorConstants.SCHEMA_NAME);
        try {
            // Getting the in-progress transaction properties
            Map<String, String> properties = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);
            // Logging the in-progress transaction properties
            CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());
            // Getting the payload metadata
            PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, properties);
            commitByProfile(request, response, query, schemaName, sqlConnection,
                    transactionDatabaseConnectorConnection.getSchemaName(), payloadMetadata);
        }
        catch (ConnectorException e) {
            // Adding the exceptions to the response
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }
}
