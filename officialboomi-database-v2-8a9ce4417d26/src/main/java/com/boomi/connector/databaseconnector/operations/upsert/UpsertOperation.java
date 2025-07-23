// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.util.DatabaseUtil;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.util.SizeLimitedUpdateOperation;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

/**
 * The Class UpsertOperation.
 *
 * @author swastik.vn
 */
public class UpsertOperation extends SizeLimitedUpdateOperation {

	/**
	 * Instantiates a new upsert operation.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public UpsertOperation(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/**
	 * Execute upsert query.
	 *
	 * @param request  the request
	 * @param response the response
	 */
	@Override
	public void executeSizeLimitedUpdate(UpdateRequest request, OperationResponse response) {
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();

		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			executeSizeLimitedUpdateForUpsert(request, response, sqlConnection,
					databaseConnectorConnection.getSchemaName(), databaseConnectorConnection.getReadTimeOut(), null);
			DatabaseUtil.commit(sqlConnection);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}

	/**
	 * Method to determine whether an upsert operation should be committed
	 *
	 * @return false when not part of a transaction
	 */
	protected boolean shouldCommit() {
		return false;
	}

	/**
	 * Method handles the statement executions based on type of database
	 *
	 * @param request
	 * @param response
	 * @param sqlConnection
	 * @param schemaNameFromConnection
	 * @param readTimeOut
	 * @param payloadMetadata
	 */
	protected void executeSizeLimitedUpdateForUpsert(UpdateRequest request, OperationResponse response,
			Connection sqlConnection, String schemaNameFromConnection, Long readTimeOut,
			PayloadMetadata payloadMetadata) {

		try {
			if (sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}

			Long batchCount = getContext().getOperationProperties().getLongProperty(
					DatabaseConnectorConstants.BATCH_COUNT);
			String commitOption = getContext().getOperationProperties()
					.getProperty(DatabaseConnectorConstants.COMMIT_OPTION);
			String schemaName = (String) getContext().getOperationProperties()
					.get(DatabaseConnectorConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, schemaNameFromConnection);
			sqlConnection.setAutoCommit(false);

			String schema = QueryBuilderUtil.getSchemaFromConnection(
					sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
					schemaNameFromConnection);
			String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
			int readTimeout = (readTimeOut != null) ? readTimeOut.intValue() : 0;
			Set<String> tableColumns = QueryBuilderUtil.retrieveTableColumns(getContext(), sqlConnection, schema);
			if (DatabaseConnectorConstants.MYSQL.equals(databaseName)) {
				MysqlUpsert upsert = new MysqlUpsert(sqlConnection, batchCount, getContext().getObjectTypeId(),
						commitOption, tableColumns);
				upsert.executeStatements(request, response, readTimeout, schema, payloadMetadata);
			} else if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
				PostgresUpsert upsert = new PostgresUpsert(sqlConnection, batchCount, getContext().getObjectTypeId(),
						commitOption, tableColumns);
				upsert.executeStatements(request, response, readTimeout, schema, payloadMetadata);
			} else {
				CommonUpsert upsert = new CommonUpsert(sqlConnection, batchCount, getContext().getObjectTypeId(),
						commitOption, schema, shouldCommit(), tableColumns);
				upsert.executeStatements(request, response, readTimeout, null, payloadMetadata);
			}
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
