// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DELETE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.GET;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.MYSQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.ORACLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.PARAM;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.POSTGRESQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.QUERY_INITIAL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.QUERY_VALUES;

/**
 * The Class ImportableUtil. 
 */
public class ImportableUtil {

	/** The _sqlConnection. */
	private Connection _sqlConnection;

	/** The object type id. */
	private String _objectTypeId;

	/**
	 * Instantiates a new importable util.
	 *
	 * @param sqlConnection       the _sqlConnection
	 * @param tableName the table name
	 */
	public ImportableUtil(Connection sqlConnection, String tableName) {
		this._sqlConnection = sqlConnection;
		this._objectTypeId = tableName;
	}

	/**
	 * Builds the importable fields.
	 *
	 * @param opsType       the ops type
	 * @param customOpsType the custom ops type
	 * @param schemaName the schema name
	 * @return the string
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	public String buildImportableFields(OperationType opsType, String customOpsType,
										String schemaName) throws SQLException {
		StringBuilder query = new StringBuilder("");
		String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
		if (OperationType.CREATE.equals(opsType)) {
			query.append(QUERY_INITIAL).append(QueryBuilderUtil.checkTableName(_objectTypeId, databaseName, schemaName));
			buildImportableInsert(query, _objectTypeId, schemaName);
		} else if (OperationType.UPDATE.equals(opsType)) {
			query.append("UPDATE ").append(QueryBuilderUtil.checkTableName(_objectTypeId, databaseName, schemaName))
					.append(" SET ");
			buildImportableUpdateSet(query, "", schemaName).append((DatabaseConnectorConstants.WHERE.toUpperCase()));
			if (ORACLE.equals(databaseName) && _objectTypeId.contains("//")) {
				_objectTypeId = _objectTypeId.replace("//", "/");
			}
			else if((MYSQL.equals(databaseName) || POSTGRESQL.equals(databaseName)) && _objectTypeId.contains("\\\\")) {
				_objectTypeId = _objectTypeId.replace("\\\\", "\\");
			}
			
			buildImportableUpdate(query, true, "", schemaName);
		} else if (DELETE.equalsIgnoreCase(customOpsType)) {
			query.append("DELETE FROM ").append(QueryBuilderUtil.checkTableName(_objectTypeId, databaseName, schemaName))
					.append(DatabaseConnectorConstants.WHERE.toUpperCase());
			buildImportableUpdate(query, true, customOpsType, schemaName);
		} else if (GET.equalsIgnoreCase(customOpsType)) {
			query.append("SELECT * FROM ").append(QueryBuilderUtil.checkTableName(_objectTypeId, databaseName, schemaName))
					.append(DatabaseConnectorConstants.WHERE.toUpperCase());
			buildImportableUpdate(query, true, customOpsType, schemaName);
		}
		return query.toString();
	}


	/**
	 * Builds the importable update.
	 *
	 * @param query         the query
	 * @param and           the and
	 * @param customOpsType the custom ops type
	 * @param schemaName the schema name
	 * @throws SQLException the SQL exception
	 */
	public void buildImportableUpdate(StringBuilder query, boolean and, String customOpsType, String schemaName)
			throws SQLException {
		String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
		try (ResultSet set = _sqlConnection.getMetaData().getColumns(_sqlConnection.getCatalog(),
				schemaName, QueryBuilderUtil.checkSpecialCharacterInDb(_objectTypeId,databaseName), null)) {
			boolean suffix = false;
			while (set.next()) {
				if (suffix) {
					appendSuffix(query, and);
				}
				query.append(set.getString(COLUMN_NAME));
				if (customOpsType.equals("GET")) {
					query.append("=$").append(set.getString(COLUMN_NAME));
				} else {
					query.append("=?");
				}
				suffix = true;
			}
		}
		
	}

	/**
	 * Builds the importable update for set clause.
	 *
	 * @param query         the query
	 * @param customOpsType the custom ops type
	 * @param schemaName    the schema name
	 * @return query 		the query string
	 * @throws SQLException the SQL exception
	 */
	public StringBuilder buildImportableUpdateSet(StringBuilder query, String customOpsType,
												  String schemaName) throws SQLException {
		String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
		try (ResultSet set = _sqlConnection.getMetaData().getColumns(_sqlConnection.getCatalog(),
				schemaName, QueryBuilderUtil.checkSpecialCharacterInDb(_objectTypeId,databaseName), null)) {
			boolean suffix = false;
			while (set.next()) {
				if (suffix) {
					query = query.append(", ");
				}
				query.append(set.getString(COLUMN_NAME));
				if (customOpsType.equals("GET")) {
					query.append("=$").append(set.getString(COLUMN_NAME));
				} else {
					query.append("=?");
				}
				suffix = true;
			}
		}
		return query;
	}

	/**
	 * Builds the importable insert.
	 *
	 * @param query         the query
	 * @param schemaName    the schema name
	 * @param objectTypeId  the Object Type ID
	 * @return query 		the Query String
	 * @throws SQLException the SQL exception
	 */
	public void buildImportableInsert(StringBuilder query, String objectTypeId, String schemaName) throws SQLException {
		String databaseName = _sqlConnection.getMetaData().getDatabaseProductName();
		int paramCount = 0;
		query.append(" (");
		DatabaseMetaData md = _sqlConnection.getMetaData();
		try (ResultSet resultSet = md.getColumns(_sqlConnection.getCatalog(), schemaName,
				QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId,databaseName), null);) {
			while (resultSet.next()) {
				paramCount++;
				query.append(resultSet.getString(COLUMN_NAME));
				query.append(",");
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append(QUERY_VALUES);
		for (int i = 1; i <= paramCount; i++) {
			query.append(PARAM);
		}
		query.deleteCharAt(query.length() - 1);
		query.append(")");
	}

	
	/**
	 * Append suffix.
	 *
	 * @param query the query
	 * @param and   the and
	 */
	private static void appendSuffix(StringBuilder query, boolean and) {
		if (and) {
			query.append(" AND ");
		} else {
			query.append(", ");
		}
	}

}
