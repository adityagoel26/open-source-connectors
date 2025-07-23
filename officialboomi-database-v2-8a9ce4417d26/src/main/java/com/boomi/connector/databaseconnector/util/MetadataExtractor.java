// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DATA_TYPE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DECIMAL_DIGITS;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.DOT;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.MSSQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TYPE_NAME;

/**
 * The Class MetadataUtil.
 *
 * @author swastik.vn
 */
public class MetadataExtractor {
	
	/** The data types. */
	private Map<String, String> _dataTypes = new HashMap<>();

	/** The type names. */
	private Map<String, String> _typeNames = new LinkedHashMap<>();
	
	/**
	 * Instantiates a new metadata util.
	 *
	 * @param sqlConnection the connection
	 * @param objectTypeId the object type id
	 * @param schemaName   the schema name
	 * @throws SQLException 
	 */
	public MetadataExtractor(Connection sqlConnection, String objectTypeId, String schemaName) throws SQLException {
		this._schemaName = schemaName;
		this._dataTypes = this.getDataTypes(sqlConnection, objectTypeId);
	}

	/** The Constant VARCHAR. */
	public static final String VARCHAR = "12";

	/** The Constant CLOB. */
	public static final String CLOB = "2005";

	/** The Constant LONGVARCHAR. */
	public static final String LONGVARCHAR = "-1";

	/** The Constant INTEGER. */
	public static final String INTEGER = "4";

	/** The Constant NUMERIC. */
	public static final String NUMERIC = "2";

	/** The Constant TIMESTAMP. */
	public static final String TIMESTAMP = "93";

	/** The Constant DATE. */
	public static final String DATE = "91";

	/** The Constant TINYINT. */
	public static final String TINYINT = "-6";

	/** The Constant BOOLEAN. */
	public static final String BOOLEAN = "16";

	/** The Constant BIT. */
	public static final String BIT = "-7";

	/** The Constant TIME. */
	public static final String TIME = "92";

	/** The Constant NVARCHAR. */
	public static final String NVARCHAR = "-9";

	/** The Constant TINYINT. */
	public static final String SMALLINT = "5";
	
	/** The Constant TINYINT. */
	public static final String CHAR = "1";
	
	/** The Constant TINYINT. */
	public static final String DECIMAL = "3";
	
	/** The Constant DOUBLE. */
	public static final String DOUBLE = "8";
	
	/** The Constant FLOAT. */
	public static final String FLOAT = "6";
	
	/** The Constant BIGINT. */
	public static final String BIGINT = "-5";
	
	/** The Constant BLOB. */
	public static final String BLOB = "2004";
	
	/** The Constant NCHAR. */
	public static final String NCHAR = "-15";
	
	/** The Constant REAL. */
	public static final String REAL = "7";
	
	/** The Constant BINARY. */
	public static final String BINARY = "-2";
	
	/** The Constant VARBINARY. */
	public static final String VARBINARY = "-3";
	
	/** The Constant LONGVARBINARY. */
	public static final String LONGVARBINARY = "-4";
	
	/** The Constant LONGNVARCHAR. */
	public static final String LONGNVARCHAR = "-16";
	
	/** The schema name. */
	private String _schemaName;


	
	/**
	 * This method will fetch the _dataTypes of the columns and stores it in a Map.
	 *
	 * @param sqlConnection the _sqlConnection
	 * @param objectTypeId the object type id
	 * @return type
	 * @throws SQLException the SQL exception
	 */
	public static Map<String, String> getDataTypesWithTable(Connection sqlConnection,
															String objectTypeId) throws SQLException {

		Map<String, String> type = new HashMap<>();
		DatabaseMetaData md = sqlConnection.getMetaData();
		String databaseName = md.getDatabaseProductName();
		try (ResultSet resultSet1 = md.getColumns(null, null,
				QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId,databaseName), null);) {
			while (resultSet1.next()) {
				if (resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.JSON)) {
						type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.JSON);
				} else if (resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.NVARCHAR)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.NVARCHAR);
				} else if (isStringDataType(resultSet1)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.STRING);
				} else if (resultSet1.getString(DATA_TYPE).equals(INTEGER) ||
						resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.BINARY_DOUBLE)
						|| resultSet1.getString(DATA_TYPE).equals(TINYINT) || resultSet1.getString(DATA_TYPE).equals(SMALLINT)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.INTEGER);
				} else if (resultSet1.getString(DATA_TYPE).equals(DATE)
						||resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.DATE)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DATE);
				} else if (resultSet1.getString(DATA_TYPE).equals(BOOLEAN)
						|| resultSet1.getString(DATA_TYPE).equals(BIT)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.BOOLEAN);
				} else if (resultSet1.getString(DATA_TYPE).equals(TIME)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.TIME);
				} else if (resultSet1.getString(DATA_TYPE).equals(BIGINT)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.LONG);
				} else if (resultSet1.getString(DATA_TYPE).equals(DOUBLE) || resultSet1.getString(DATA_TYPE).equals(DECIMAL)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DOUBLE);
				}  else if (resultSet1.getString(DATA_TYPE).equals(FLOAT) || resultSet1.getString(DATA_TYPE).equals(REAL)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.FLOAT);
				} else if (resultSet1.getString(TYPE_NAME).equals(DatabaseConnectorConstants.BLOB)
						|| resultSet1.getString(DATA_TYPE).equals(BINARY) || resultSet1.getString(DATA_TYPE).equals(LONGVARBINARY)
						|| resultSet1.getString(DATA_TYPE).equals(VARBINARY)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.BLOB);
				} else if (resultSet1.getString(DATA_TYPE).equals(TIMESTAMP)) {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.TIMESTAMP);
				} else if (resultSet1.getString(DATA_TYPE).equals(NUMERIC)) {
					int decimalDigits = resultSet1.getInt(DECIMAL_DIGITS);
					  if(decimalDigits == 0) {
						  type.put(objectTypeId + DOT + resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.INTEGER);
					  } else {
						  type.put(objectTypeId + DOT + resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DOUBLE);
					  }
					 
				} else {
					type.put(objectTypeId+DOT+resultSet1.getString(COLUMN_NAME), null);
				}				
			}
		}

		return type;

	}
	
	private Map<String, String> getDataTypes(Connection con, String objectTypeId) throws SQLException {
		Map<String, String> type = new HashMap<>();
		DatabaseMetaData md = con.getMetaData();
		String databaseName = md.getDatabaseProductName();
		try (ResultSet resultSet1 = md.getColumns(con.getCatalog(), this._schemaName,
				QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId,databaseName), null);) {
			while (resultSet1.next()) {
				if (resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.JSON)) {
						type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.JSON);
				} else if (resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.NVARCHAR)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.NVARCHAR);
				} else if (isStringDataType(resultSet1)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.STRING);
				} else if (resultSet1.getString(DATA_TYPE).equals(INTEGER)
						|| resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.BINARY_DOUBLE)
						|| resultSet1.getString(DATA_TYPE).equals(TINYINT) || resultSet1.getString(DATA_TYPE).equals(SMALLINT)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.INTEGER);
				} else if (resultSet1.getString(DATA_TYPE).equals(DATE) ||
						resultSet1.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.DATE) ) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DATE);
				} else if (resultSet1.getString(DATA_TYPE).equals(BOOLEAN)
						|| resultSet1.getString(DATA_TYPE).equals(BIT)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.BOOLEAN);
				} else if (resultSet1.getString(DATA_TYPE).equals(TIME)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.TIME);
				}  else if (resultSet1.getString(DATA_TYPE).equals(BIGINT)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.LONG);
				} else if (resultSet1.getString(DATA_TYPE).equals(DOUBLE) || resultSet1.getString(DATA_TYPE).equals(DECIMAL)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DOUBLE);
				}  else if (resultSet1.getString(DATA_TYPE).equals(FLOAT) || resultSet1.getString(DATA_TYPE).equals(REAL)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.FLOAT);
				} else if (resultSet1.getString(TYPE_NAME).equals(DatabaseConnectorConstants.BLOB)
						|| resultSet1.getString(DATA_TYPE).equals(BINARY) || resultSet1.getString(DATA_TYPE).equals(LONGVARBINARY)
						|| resultSet1.getString(DATA_TYPE).equals(VARBINARY)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.BLOB);
				} else if (resultSet1.getString(DATA_TYPE).equals(TIMESTAMP)) {
					type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.TIMESTAMP);
				} else if (resultSet1.getString(DATA_TYPE).equals(NUMERIC)) {
					int decimalDigits = resultSet1.getInt(DECIMAL_DIGITS);
					  if(decimalDigits == 0) {
						  type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.INTEGER);
					  } else {
						  type.put(resultSet1.getString(COLUMN_NAME), DatabaseConnectorConstants.DOUBLE);
					  }
					 
				} 
				
				else {
					type.put(resultSet1.getString(COLUMN_NAME), null);
				}
				_typeNames.put(resultSet1.getString(COLUMN_NAME), resultSet1.getString(TYPE_NAME));
			}
			if (md.getDatabaseProductName().equals(MSSQL) && type.isEmpty()) {
				throw new ConnectorException(objectTypeId+ " does not exist");
			}
		}
		return type;

	}

	/**
	 * This method will return the true false value based on the comparison of data types
	 * @param resultSet
	 * @return boolean value
	 * @throws SQLException
	 */
	private static boolean isStringDataType(ResultSet resultSet) throws SQLException {
		if( resultSet == null) {
			return false;
		}
		return  resultSet.getString(DATA_TYPE).equals(VARCHAR)
				|| resultSet.getString(DATA_TYPE).equals(LONGVARCHAR)
				|| resultSet.getString(DATA_TYPE).equals(NVARCHAR)
				|| resultSet.getString(TYPE_NAME).equalsIgnoreCase(DatabaseConnectorConstants.CLOB)
				|| resultSet.getString(DATA_TYPE).equals(CHAR)
				|| resultSet.getString(DATA_TYPE).equals(NCHAR)
				|| resultSet.getString(DATA_TYPE).equals(LONGNVARCHAR)
				|| resultSet.getString(DATA_TYPE).equals(String.valueOf(Types.ROWID));
	}
	
	/**
	 * Gets the data type.
	 *
	 * @return the data type
	 */
	public Map<String, String> getDataType() {
		return _dataTypes;
	}
	/**
	 * Gets the type names.
	 *
	 * @return the type names
	 */
	public Map<String, String> getTypeNames() {
		return _typeNames;
	}

}