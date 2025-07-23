// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.update;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.util.CustomResponseUtil;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.MetadataExtractor;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.SCHEMA_NAME;

/**
 * The type Dynamic update transaction operation.
 */
public class DynamicUpdateTransactionOperation extends DynamicUpdateOperation {

    /**
     * Instantiates a new dynamic update operation.
     *
     * @param connection the connection
     */
    public DynamicUpdateTransactionOperation(DatabaseConnectorConnection connection) {
        super(connection);
    }

    @Override
    public TransactionDatabaseConnectorConnection getConnection() {
        return (TransactionDatabaseConnectorConnection) super.getConnection();
    }

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
            if (sqlConnection == null) {
                throw new ConnectorException(
                        DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
            }
            DatabaseUtil.setupConnection(getContext(), sqlConnection, transactionDatabaseConnectorConnection);

            List<ObjectData> batchData = new ArrayList<>();
            for (ObjectData objdata : request) {
                batchData.add(objdata);
            }
            DatabaseMetaData databaseMetaData = sqlConnection.getMetaData();
            //get schemaName from the operation properties
            String schemaName = getContext().getOperationProperties()
                    .getProperty(SCHEMA_NAME);
            //get schema name from the connection
            String schema = QueryBuilderUtil.
                    getSchemaFromConnection(databaseMetaData.getDatabaseProductName(), sqlConnection,
                    schemaName,getConnection().getSchemaName());
            // This Map will be getting the datatype of the each column associated with the
            // table.
            Map<String, String> dataType =
                    new MetadataExtractor(sqlConnection, getContext().getObjectTypeId(), schema).getDataType();
            String databaseName = databaseMetaData.getDatabaseProductName();
            //build the query specific to update operation
            StringBuilder query = getInitialQuery(QueryBuilderUtil
                    .checkTableName(getContext().getObjectTypeId(), databaseName, schema));
            //append question marks based on column name
            this.appendKeys(batchData, query, response);
            int readTimeout = getConnection().getReadTimeOut() != null ?
                    getConnection().getReadTimeOut().intValue() : 0;
            Map<String, String> properties      = CustomResponseUtil
                    .getInProgressTransactionProperties(transactionCacheKey);
            Set<String> columnNames = QueryBuilderUtil.retrieveTableColumns(getContext(), sqlConnection, schema);
            for (ObjectData data : batchData) {
                //Execute the specified Query but do not commit
                try {
                    int updatedRowCount = executeNonBatchUpdate(data, sqlConnection, dataType, query,
                            readTimeout, columnNames);
                    QueryResponse queryResponse = new QueryResponse(query.toString(), updatedRowCount,
                            "Executed Successfully");
                    //log the properties in process logs
                    CustomResponseUtil.logInProgressTransactionProperties(transactionCacheKey, response.getLogger());
                    //create metadata from response
                    PayloadMetadata payloadMetadata = CustomResponseUtil.createMetadata(response, properties);
                    CustomResponseUtil.handleSuccess(data, response, payloadMetadata, queryResponse);
                } catch (SQLException e) {
                    CustomResponseUtil.writeSqlErrorResponse(e, data, response);
                } catch (IOException | IllegalArgumentException | ConnectorException e) {
                    CustomResponseUtil.writeErrorResponse(e, data, response);
                }
            }

        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }

    /**
     * Overrides parent method and returns false in {@link DynamicUpdateTransactionOperation} class.
     * @return
     */
    @Override
    protected boolean shouldCommit() {
        return false;
    }

}