// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.storedprocedureoperation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;

import java.sql.Connection;

/**
 * The Class StoredProcedureOperation.
 *
 * @author swastik.vn
 */
public class StoredProcedureOperation extends SizeLimitedUpdateOperation {

    /**
     * Instantiates a new stored procedure operation.
     *
     * @param databaseConnectorConnection the databaseConnectorConnection
     */
    public StoredProcedureOperation(DatabaseConnectorConnection databaseConnectorConnection) {
        super(databaseConnectorConnection);
    }

    /**
     * Execute size limited update.
     *
     * @param request  the request
     * @param response the response
     */
    @Override
    public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
        DatabaseConnectorConnection databaseConnectorConnection = getConnection();
        String procedureName = getContext().getObjectTypeId();
        try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
            if (sqlConnection == null) {
                throw new ConnectorException(DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
            }
            if (!DatabaseConnectorConstants.POSTGRESQL.equalsIgnoreCase
                    (sqlConnection.getMetaData().getDatabaseProductName())) {
                sqlConnection.setAutoCommit(false);
            }
            String schemaName = (String) getContext().getOperationProperties().get(
                    DatabaseConnectorConstants.SCHEMA_NAME);
            QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName,
                    databaseConnectorConnection.getSchemaName());
            Long batchCount = getContext().getOperationProperties()
                    .getLongProperty(DatabaseConnectorConstants.BATCH_COUNT);
            Long maxFieldSize = getContext().getOperationProperties().getLongProperty(
                    DatabaseConnectorConstants.MAX_FIELD_SIZE);
            Long fetchSize = getContext().getOperationProperties().getLongProperty(
                    DatabaseConnectorConstants.FETCH_SIZE);
            String schema = QueryBuilderUtil.getSchemaFromConnection(
                    sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
                    getConnection().getSchemaName());
            StoredProcedureExecute execute = new StoredProcedureExecute(sqlConnection, procedureName, request, response,
                    getContext(), schema);
            int readTimeout =
                    getConnection().getReadTimeOut() != null ? getConnection().getReadTimeOut().intValue() : 0;
            execute.executeStatements(batchCount, maxFieldSize, fetchSize, readTimeout);
        } catch (Exception e) {
            ResponseUtil.addExceptionFailures(response, request, e);
        }
    }

    /**
     * Gets the Connection instance.
     *
     * @return the connection
     */
    @Override
    public DatabaseConnectorConnection getConnection() {
        return (DatabaseConnectorConnection) super.getConnection();
    }
}