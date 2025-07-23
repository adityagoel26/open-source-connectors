// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * Dynamic Insert Operation capable of being part of a transaction.
 */
public class DynamicInsertTransactionOperation extends DynamicInsertOperation {

    /**
     * Instantiates a new standard insert transaction operation.
     *
     * @param transactionConnection the connection
     */
    public DynamicInsertTransactionOperation(TransactionDatabaseConnectorConnection transactionConnection) {
        super(transactionConnection);
    }

    /**
     * Returns the {@link TransactionDatabaseConnectorConnection} instance.
     * @return
     */
    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

    /**
     * Execute the {@link DynamicInsertTransactionOperation}
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
        String schemaName = getContext().getOperationProperties().getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
        int readTimeout =
                (transactionDatabaseConnectorConnection.getReadTimeOut() != null) ? getConnection().getReadTimeOut()
                        .intValue() : 0;
        // Getting the in-progress transaction properties
        Map<String, String> properties = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);
        // Logging the in-progress transaction properties
        CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());
        // Creating the payload metadata
        PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, properties);

        try {
            QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                    transactionDatabaseConnectorConnection.getSchemaName());
            String objectTypeId = getContext().getObjectTypeId();
            String schema = QueryBuilderUtil.getSchemaFromConnection(
                    sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
                    getConnection().getSchemaName());
            MetadataExtractor metadataExtractor = new MetadataExtractor(sqlConnection, objectTypeId,
                    schema);
            Map<String, String> dataTypes = metadataExtractor.getDataType();

            // Validates that the specified objectTypeId exists in the schema for further steps
            //Other dynamic operations will be fixed as part of CONC-8148
            QueryBuilderUtil.validateDataTypeMappings(dataTypes, objectTypeId);

            Map<String, String>  typeNames = metadataExtractor.getTypeNames();
            String autoIncrementColumn = QueryBuilderUtil.getAutoIncrementColumn(sqlConnection, objectTypeId, schema);
            commitByProfile(sqlConnection, request, response, schema, dataTypes, typeNames, readTimeout,
                    autoIncrementColumn, payloadMetadata);
        } catch (SQLException sqlException) {
            // Add any SQL exceptions to the response
            ResponseUtil.addExceptionFailures(response, request, sqlException);
        }
    }
}
