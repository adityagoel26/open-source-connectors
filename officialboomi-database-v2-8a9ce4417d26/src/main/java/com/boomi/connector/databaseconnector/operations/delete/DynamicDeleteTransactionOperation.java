// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.delete;

import com.boomi.connector.api.ObjectData;
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
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The type Dynamic delete transaction operation.
 */
public class DynamicDeleteTransactionOperation extends DynamicDeleteOperation {


    /**
     * Instantiates a new dynamic delete operation.
     *
     * @param transactionDatabaseConnectorConnection the transactionDatabaseConnectorConnection
     */
    public DynamicDeleteTransactionOperation(
            TransactionDatabaseConnectorConnection transactionDatabaseConnectorConnection) {
        super(transactionDatabaseConnectorConnection);
    }

    /** The Constant LOG. */
    private static final Logger LOG = Logger.getLogger(DynamicDeleteTransactionOperation.class.getName());

    /**
     * Executes a size-limited update operation on the database.
     *
     * @param request the {@link UpdateRequest} containing the data to be updated
     * @param response the {@link OperationResponse} to be populated with the results of the update operation
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

        try {
            DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
            //get schemaName from the operation properties
            String schemaName = getContext().getOperationProperties()
                    .getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
            QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                    transactionDatabaseConnectorConnection.getSchemaName());
            sqlConnection.setAutoCommit(false);
            //get schema name from the connection
            String schema = QueryBuilderUtil.getSchemaFromConnection(databaseMetaData.getDatabaseProductName(),
                    sqlConnection, schemaName, transactionDatabaseConnectorConnection.getSchemaName());
            String databaseName = databaseMetaData.getDatabaseProductName();
            int readTimeout =
                    (getConnection().getReadTimeOut() != null) ? getConnection().getReadTimeOut().intValue() : 0;

            //build the query specific to delete operation
            StringBuilder query = new StringBuilder(DatabaseConnectorConstants.DELETE_QUERY +
                    QueryBuilderUtil.checkTableName(getContext().getObjectTypeId(), databaseName, schema));
            List<ObjectData> trackedData = getObjectDataList(request);

            //append question marks based on column name
            appendKeys(trackedData, query);

            //extract the data types from database metadata
            Map<String, String> dataTypes = new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(),
                    schema).getDataType();
            Map<String,String> trackedProperties = CustomResponseUtil.
                    getInProgressTransactionProperties(transactionCacheKey);

            //log the properties in process logs
            CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());

            //create metadata from response
            PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, trackedProperties);

            //Execute the specified Query but do not commit
            commitByProfile(sqlConnection, trackedData, response, query, readTimeout, dataTypes, payloadMetadata);
            LOG.log(Level.INFO, "Statements Executed!!");
        } catch (SQLException e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }

    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

}
