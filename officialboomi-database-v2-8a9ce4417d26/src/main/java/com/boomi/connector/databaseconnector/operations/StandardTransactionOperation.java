// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;

import java.sql.Connection;
import java.util.Map;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.QUERY;

/**
 * The Class StandardTransactionOperation.
 */
public class StandardTransactionOperation extends StandardOperation {
    /**
     * Instantiates a new standard operation.
     *
     * @param connection the connection
     */
    public StandardTransactionOperation(DatabaseConnectorConnection connection) {
        super(connection);
    }

    /**
     * Gets the connection.
     *
     * @return the connection
     */
    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

    /**
     * Execute size limited update.
     * @param request  the request
     * @param response the response
     */
    @Override
    public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
        TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection = getConnection();
        String query = getContext().getOperationProperties().getProperty(QUERY, "");
        //get schemaName from the operation properties
        String schemaName = (String) getContext().getOperationProperties()
                .get(DatabaseConnectorConstants.SCHEMA_NAME);
        try {
            //create a cache key for transaction
            TransactionCacheKey transactionCacheKey = transactionDatabaseConnectorConnection.createCacheKey(
                    request.getTopLevelExecutionId());
            //get active connection object from connector cache
            Connection sqlConnection = transactionDatabaseConnectorConnection.getDatabaseConnection(
                    transactionCacheKey);
            CustomResponseUtil.logJoinTransactionStatus(response.getLogger(), transactionCacheKey.toString());

            sqlConnection.setAutoCommit(false);
            QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                    transactionDatabaseConnectorConnection.getSchemaName());
            //get schema name from the connection
            String schema = QueryBuilderUtil.getSchemaFromConnection(
                    sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
                    transactionDatabaseConnectorConnection.getSchemaName());
            Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
                    schema).getDataType();
            Map<String, String> properties = CustomResponseUtil.getInProgressTransactionProperties(transactionCacheKey);

            for (ObjectData objdata : request) {
                //Execute the specified Query but do not commit
                int updatedRowCount = executeNonBatch(sqlConnection, objdata, response, query, dataTypes);
                QueryResponse queryResponse = new QueryResponse(query, updatedRowCount, "Executed Successfully");
                //log the properties in process logs
                CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());
                //create metadata from response
                PayloadMetadata     payloadMetadata = CustomResponseUtil.createMetadata(response, properties);
                CustomResponseUtil.handleSuccess(objdata, response, payloadMetadata, queryResponse);
            }

        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }
}