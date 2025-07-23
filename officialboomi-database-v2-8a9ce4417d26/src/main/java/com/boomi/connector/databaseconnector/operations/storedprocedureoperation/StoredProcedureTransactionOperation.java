// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.storedprocedureoperation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;

import java.sql.Connection;
import java.util.Map;

/**
 * Stored Procedure Operation capable of being part of a transaction.
 */
public class StoredProcedureTransactionOperation extends StoredProcedureOperation{

    /**
     * Instantiates a new stored procedure operation.
     *
     * @param transactionConnection the TransactionDatabaseConnectorConnection
     */
    public StoredProcedureTransactionOperation(TransactionDatabaseConnectorConnection transactionConnection) {
        super(transactionConnection);
    }

    /**
     * @param request  the request
     * @param response the response
     */
    @Override
    public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
        // Getting the transaction database connector connection
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = getConnection();
        // Create a cache key for the transaction
        TransactionCacheKey transactionCacheKey = transactionDatabaseConnectorConnection.createCacheKey(
                request.getTopLevelExecutionId());
        // Getting the database connection from the cache
        Connection sqlConnection = transactionDatabaseConnectorConnection.getDatabaseConnection(transactionCacheKey);
        CustomResponseUtil.logJoinTransactionStatus(response.getLogger(), transactionCacheKey.toString());
        String procedureName = getContext().getObjectTypeId();

        // Getting the in-progress transaction properties
        Map<String, String> properties = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);
        // Logging the in-progress transaction properties
        CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());
        // Creating the payload metadata
        PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, properties);

        try {

            if (sqlConnection == null) {
                throw new ConnectorException(DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
            }
            StringBuilder query;
            sqlConnection.setAutoCommit(false);
            PropertyMap operationProperties = getContext().getOperationProperties();
            String schemaName = (String) operationProperties.get(
                    DatabaseConnectorConstants.SCHEMA_NAME);
            QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                    transactionDatabaseConnectorConnection.getSchemaName());
            Long maxFieldSize = operationProperties.getLongProperty(
                    DatabaseConnectorConstants.MAX_FIELD_SIZE);
            Long fetchSize = operationProperties.getLongProperty(
                    DatabaseConnectorConstants.FETCH_SIZE);
            String schema = QueryBuilderUtil.getSchemaFromConnection(
                    sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
                    getConnection().getSchemaName());
            StoredProcedureExecute execute = new StoredProcedureExecute(sqlConnection, procedureName, request, response,
                    getContext(), schema);
            int readTimeout =
                    (getConnection().getReadTimeOut() == null) ? 0 : getConnection().getReadTimeOut().intValue();
            query = execute.getQuery();
            execute.doNonBatch(query, readTimeout, maxFieldSize, fetchSize, payloadMetadata);
        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }

    /**
     * @return TransactionDatabaseConnectorConnection
     */
    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }
}
