// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import oracle.jdbc.OracleType;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.json.JSONObject;
import org.postgresql.util.PGobject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The Class QueryBuilderUtil.
 *
 * @author swastik.vn
 */
public class QueryBuilderUtil {

	/** The Regular expression to find the where and order by properties. */
	private static final String PARAM_REGEX = "'[ ]{0,}\\$[\\w_]+[ ]{0,}'|\\$[\\w_]+|\"[ ]{0,}\\$[\\w_]+[ ]{0,}\"";
	private static final Logger LOG = Logger.getLogger(QueryBuilderUtil.class.getName());
	private static final int THOUSAND_MILLISECONDS = 1000;
	private static final float THOUSAND_MILLISECONDS_FLOAT = 1000.0f;

	/**
	 * Instantiates a new query builder util.
	 */
	private QueryBuilderUtil() {

	}

	/**
	 * Utility method to build the Query for procedure call.
	 *
	 * @param params     the params
	 * @param objectType the object type
	 * @return the string builder
	 */
	public static StringBuilder buildProcedureQuery(List<String> params, String objectType) {
		StringBuilder query = new StringBuilder("call ").append(objectType).append("(");
		if (!params.isEmpty()) {
			for (int i = 1; i <= params.size(); i++) {
				query.append("?");
				query.append(",");
			}
			query.deleteCharAt(query.length() - 1);
		}
		query.append(")");

		return query;
	}

	/**
	 * This Method will build the Procedure query for Microsoft SQL database.
	 *
	 * @param params   the params
	 * @param objectId the object id
	 * @param schemaName the schema name
	 * @return the string builder
	 */
	public static StringBuilder buildInitialQuerySqlDB(List<String> params, String objectId, String schemaName) {
		StringBuilder query = new StringBuilder();
		if(schemaName != null && !DatabaseConnectorConstants.MSSQL.equalsIgnoreCase(schemaName)) {
			query.append("{call ").append(schemaName).append(".").append(objectId);
		}else {
			query.append("{call ").append(objectId);
		}
		if (!params.isEmpty()) {
			query.append("(");
			for (int i = 0; i <= params.size() - 1; i++) {
				query.append(" ");
				query.append("?,");
			}
			query.deleteCharAt(query.length() - 1);
			query.append(")");
		}
		query.append("};");
		return query;
	}

	/**
	 * Utility method to check for special character in table name.
	 * @param databaseName the database name
	 * @param schemaName the schema name
	 * @param objectTypeId   the object type ID
	 *
	 * @return the string
	 */
	public static String checkTableName(String objectTypeId, String databaseName, String schemaName) {
		if (objectTypeId != null && (objectTypeId.contains(".") || objectTypeId.contains("~") || objectTypeId.contains("!")
				|| objectTypeId.contains("@") || objectTypeId.contains("#") || objectTypeId.contains("$")
				|| objectTypeId.contains("%") || objectTypeId.contains("^") || objectTypeId.contains("&")
				|| objectTypeId.contains("*") || objectTypeId.contains("(") || objectTypeId.contains(")")
				|| objectTypeId.contains("{") || objectTypeId.contains("}") || objectTypeId.contains("[")
				|| objectTypeId.contains("]") || objectTypeId.contains("\\") || objectTypeId.contains("|")
				|| objectTypeId.contains(";") || objectTypeId.contains(":") || objectTypeId.contains("\"")
				|| objectTypeId.contains("'") || objectTypeId.contains("/") || objectTypeId.contains("<")
				|| objectTypeId.contains(">") || objectTypeId.contains("?"))) {
			if (DatabaseConnectorConstants.MYSQL.equals(databaseName)) {
				objectTypeId = "`" + objectTypeId + "`";
			} else if (DatabaseConnectorConstants.ORACLE.equals(databaseName)
					|| DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)
					|| DatabaseConnectorConstants.MSSQL.equals(databaseName)) {
				objectTypeId = DatabaseConnectorConstants.DOUBLE_QUOTE + objectTypeId + DatabaseConnectorConstants.DOUBLE_QUOTE;
			}
			return objectTypeId;
		} else {
			if(DatabaseConnectorConstants.MSSQL.equals(databaseName) && (schemaName != null)
                    && !DatabaseConnectorConstants.MSSQL_DEFAULT_SCHEMA.equalsIgnoreCase(schemaName)) {
				objectTypeId = schemaName + "." + objectTypeId;
			}
			return objectTypeId;
		}
	}

	/**
	 * Sanitizes database object identifiers by applying database-specific character escaping rules.
	 *
	 * @param objectTypeId The database object identifier to process
	 * @param databaseName The target database system name (MySQL, PostgreSQL, or Oracle)
	 * @return The sanitized object identifier with appropriate escaping for the specified database
	 */
	public static String checkSpecialCharacterInDb(String objectTypeId, String databaseName) {
		if (StringUtils.isEmpty(objectTypeId)) {
			return objectTypeId;
		}

		switch (databaseName) {
			case DatabaseConnectorConstants.MYSQL:
			case DatabaseConnectorConstants.POSTGRESQL:
				return sanitizeObjectId(objectTypeId, databaseName);
			case DatabaseConnectorConstants.ORACLE:
				return sanitizeObjectIdForOracle(objectTypeId);
			default:
				return objectTypeId;
		}
	}

	/**
	 * Sanitizes database object identifiers based on database type.
	 */
    private static String sanitizeObjectId(String objectTypeId, String databaseName) {
		// Escape backslashes
		if (objectTypeId.contains("\\")) {
			objectTypeId = objectTypeId.replace("\\", "\\\\");
		}
        // Remove surrounding quotes based on database type
        if ((DatabaseConnectorConstants.MYSQL.equals(databaseName) && objectTypeId.startsWith("`")
                && objectTypeId.endsWith("`")) || (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)
                && objectTypeId.startsWith(DatabaseConnectorConstants.DOUBLE_QUOTE) && objectTypeId.endsWith(
                DatabaseConnectorConstants.DOUBLE_QUOTE))) {
            objectTypeId = objectTypeId.substring(1, objectTypeId.length() - 1);
        }

        if (objectTypeId.contains("\\\\\\\\")) {
            objectTypeId = objectTypeId.replace("\\\\\\\\", "\\\\");
        }

        return objectTypeId;
    }

	/**
	 * Sanitizes Oracle object identifiers by handling forward slashes and quotes.
	 *
	 * @param objectTypeId the Oracle object identifier to sanitize
	 * @return sanitized object identifier with escaped slashes and removed quotes
	 */
	private static String sanitizeObjectIdForOracle(String objectTypeId) {
		// Replace `/` with `//`
		if (objectTypeId.contains("/")) {
			objectTypeId = objectTypeId.replace("/", "//");
		}
		// Remove surrounding double quotes
		if (objectTypeId.startsWith(DatabaseConnectorConstants.DOUBLE_QUOTE) &&
				objectTypeId.endsWith(DatabaseConnectorConstants.DOUBLE_QUOTE)) {
			objectTypeId = objectTypeId.substring(1, objectTypeId.length() - 1);
		}
		return objectTypeId;
	}

	/**
	 * Sets Schema Name in Connection
	 *
	 * @param con
	 * @param schemaNameFromOperation
	 * @param schemaNameFromConnectorConnection
	 * @throws SQLException
	 */
	public static void setSchemaNameInConnection(Connection con, String schemaNameFromOperation,
			String schemaNameFromConnectorConnection) throws SQLException {

		if (schemaNameFromOperation != null && schemaNameFromOperation.isEmpty()
				&& !schemaNameFromConnectorConnection.isEmpty()) {
			schemaNameFromOperation = schemaNameFromConnectorConnection;
		}
		String databaseProductName = con.getMetaData().getDatabaseProductName();

		if ((DatabaseConnectorConstants.POSTGRESQL.equals(databaseProductName)
				|| DatabaseConnectorConstants.ORACLE.equals(databaseProductName)
				|| DatabaseConnectorConstants.MSSQL.equals(databaseProductName)) && !StringUtil.isEmpty(
				schemaNameFromOperation)) {
			con.setSchema(schemaNameFromOperation);
		} else if (!StringUtil.isEmpty(schemaNameFromOperation)) {
			con.setCatalog(schemaNameFromOperation);
		}
	}

	/**
	 * This method will build the initial characters required for the insert query.
	 *
	 * @param sqlConnection the _sqlConnection
	 * @param objectTypeId the object type id
	 * @param schemaName the schema name
	 * @param columnNames the column names
	 * @return query
	 * @throws SQLException the SQL exception
	 */
	public static StringBuilder buildInitialQuery(Connection sqlConnection,
			String objectTypeId, String schemaName, Set<String> columnNames) throws SQLException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		StringBuilder query = new StringBuilder(
				DatabaseConnectorConstants.QUERY_INITIAL + checkTableName(objectTypeId, databaseName, schemaName)
						+ "(");
		for (String key : columnNames) {
				query.append(key);
				query.append(",");
			}
		query.deleteCharAt(query.length() - 1);
		query.append(DatabaseConnectorConstants.QUERY_VALUES);
		return query;
	}

	/**
	 * Builds the Insert Query Required of Prepared Statement in Dynamic Insert.
	 *
	 * @param sqlConnection          the connection
	 * @param objdata      the tracked data
	 * @param objectTypeId the object type id
	 * @param typeNames    the data type of the columns map
	 * @param autoIncrementColumn 	 the auto increment column name
	 * @param schemaName   the schema name
	 * @return the string builder
	 * @throws SQLException the SQL exception
	 * @throws IOException  Signals that an I/O exception has occurred.
	 */

	public static StringBuilder buildInitialInsertQuery(Connection sqlConnection, ObjectData objdata,
			String objectTypeId, Map<String, String> typeNames, String autoIncrementColumn, String schemaName)
			throws SQLException, IOException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		StringBuilder query = new StringBuilder(
				DatabaseConnectorConstants.QUERY_INITIAL + checkTableName(objectTypeId, databaseName, schemaName)
						+ "(");
		int          paramCount = 0;
		ObjectReader reader     = DBv2JsonUtil.getObjectReader();

		try (InputStream is = objdata.getData();) {
			JsonNode node = reader.readTree(is);
			for(Map.Entry<String, String> entries : typeNames.entrySet()) {
				String key = entries.getKey();
				if (autoIncrementColumn == null || node.get(autoIncrementColumn) != null
						|| !autoIncrementColumn.equalsIgnoreCase(key)) {
					paramCount++;
					query.append(key);
					query.append(",");
				}
			}
		}
		query.deleteCharAt(query.length() - 1);
		query.append(DatabaseConnectorConstants.QUERY_VALUES);
		for (int i = 1; i <= paramCount; i++) {
			query.append("?");
			query.append(",");
		}
		query.deleteCharAt(query.length() - 1);
		query.append(")");
		return query;
	}

	/**
	 * Retrieves the name of the auto-increment column for the specified table in the connected database.
	 * <p>
	 * This method queries the database metadata to identify the column that is marked as auto-increment
	 * for the table specified by the `objectTypeId`. If the table does not exist or if there are no
	 * columns marked as auto-increment, the method returns null.
	 *
	 * @param connection   The database connection object to be used for accessing the metadata.
	 * @param objectTypeId The name of the table for which to retrieve the auto-increment column.
	 * @param schema       The schema name
	 * @return The name of the auto-increment column, or null if no such column exists.
	 * @throws SQLException If an error occurs while retrieving the column metadata.
	 */
	public static String getAutoIncrementColumn(Connection connection, String objectTypeId, String schema)
			throws SQLException {
		DatabaseMetaData metaData = connection.getMetaData();

		try (ResultSet columns = metaData.getColumns(null, schema, objectTypeId, null)) {
			while (columns.next()) {
				if (isAutoIncrementColumn(columns)) {
					return columns.getString(DatabaseConnectorConstants.COLUMN_NAME);
				}
			}
		}
		return null;
	}

	/**
	 * Determines if the current column in the {@link ResultSet} is an auto-increment column.
	 *
	 * @param columns the {@link ResultSet} containing column metadata
	 * @return {@code true} if the column is auto-increment, {@code false} otherwise
	 * @throws SQLException if a database access error occurs
	 */
	private static boolean isAutoIncrementColumn(ResultSet columns) throws SQLException {
		String isAutoIncrement = columns.getString(DatabaseConnectorConstants.IS_AUTOINCREMENT);
		return DatabaseConnectorConstants.YES.equalsIgnoreCase(isAutoIncrement)
				|| DatabaseConnectorConstants.TRUE.equalsIgnoreCase(isAutoIncrement)
				|| DatabaseConnectorConstants.ONE.equals(isAutoIncrement);
	}

	/**
	 * Validate the table name.
	 *
	 * @param finalQuery the final query
	 * @return the string
	 * @throws IndexOutOfBoundsException the index out of bounds exception
	 */
	public static String validateTheTableName(String finalQuery) {
		int findIndex = finalQuery.lastIndexOf("FROM");
		String tep = finalQuery.substring(findIndex);
		if (tep.indexOf(" ") >= 0) {
			tep = tep.substring(tep.indexOf(" ") + 1);
			if (tep.indexOf(" ") >= 0) {
				finalQuery = tep.substring(0, tep.indexOf(" "));
			} else {
				finalQuery = tep;
			}
		}
		if (finalQuery.contains(".") && !finalQuery.startsWith(DatabaseConnectorConstants.DOUBLE_QUOTE)
				&& !finalQuery.startsWith("`")) {
			finalQuery = finalQuery.substring(finalQuery.indexOf(".") + 1, finalQuery.length());
		}
		if (finalQuery.startsWith("`") || finalQuery.startsWith(DatabaseConnectorConstants.DOUBLE_QUOTE)) {
			finalQuery = finalQuery.substring(1, finalQuery.length() - 1);
		}
		return finalQuery;
	}

	/**
	 * Checks the column data type and adds the parameter to the prepared statement.
	 *
	 * @param dataTypes the data types
	 * @param key       the key
	 * @param value     the value
	 * @param statement    the query
	 * @param index         the i
	 * @param sqlConnection the _sqlConnection
	 * @throws SQLException the SQL exception
	 * @throws IOException
	 */
	public static void checkDataType(Map<String, String> dataTypes, String key, String value, PreparedStatement statement,
			int index, Connection sqlConnection) throws SQLException, IOException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		if(dataTypes != null && !dataTypes.isEmpty()) {
			switch (dataTypes.get(key)) {
				case DatabaseConnectorConstants.INTEGER:
					setIntegerDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.DATE:
					setDateDataType(value, statement, index, databaseName);
					break;
				case DatabaseConnectorConstants.STRING:
					setStringDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.NVARCHAR:
					statement.setString(index, StringEscapeUtils.unescapeJava(value));
					break;
				case DatabaseConnectorConstants.JSON:
					setJSONDataType(value, statement, index, databaseName);
					break;
				case DatabaseConnectorConstants.TIME:
					setTimeDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.BOOLEAN:
					setBooleanDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.LONG:
					setLongDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.FLOAT:
					setFloatDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.DOUBLE:
					setDoubleDataType(value, statement, index);
					break;
				case DatabaseConnectorConstants.BLOB:
					setBLOBDataType(value, statement, index, databaseName);
					break;
				case DatabaseConnectorConstants.TIMESTAMP:
					setTimestampDataType(value, statement, index);
					break;
				default:
					break;
			}
		}
	}

	/**
	 * Method to extract JSON Value from the input request and set it to prepared
	 * statement based on the database names
	 *
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @param databaseName
	 * @throws SQLException
	 */
	public static void extractJson(String value, PreparedStatement bstmnt, int i, String databaseName)
			throws SQLException {
		if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(value);
			bstmnt.setObject(i, jsonObject);
		} else if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
			OracleJsonFactory factory = new OracleJsonFactory();
			OracleJsonObject object = factory.createObject();
			JSONObject jsonObject = new JSONObject(value);
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			bstmnt.setObject(i, object, OracleType.JSON);
		} else {
			bstmnt.setString(i, value);
		}
	}

	/**
	 * Method to extract JSON Value from the input request and set it to prepared
	 * statement based on the database names
	 *
	 * @param pstmnt
	 * @param i
	 * @param databaseName
	 * @throws SQLException
	 */
	public static void extractUnescapeJson(PreparedStatement pstmnt, int i, String databaseName, JsonNode fieldValue)
			throws SQLException {
		if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
			PGobject jsonObject = new PGobject();
			jsonObject.setType("json");
			jsonObject.setValue(fieldValue.toString());
			pstmnt.setObject(i, jsonObject);
		} else if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
			OracleJsonFactory factory = new OracleJsonFactory();
			OracleJsonObject object = factory.createObject();
			JSONObject jsonObject = new JSONObject(fieldValue.toString());
			Iterator<String> keys = jsonObject.keys();
			while (keys.hasNext()) {
				String jsonKeys = keys.next();
				object.put(jsonKeys, jsonObject.get(jsonKeys).toString());
			}
			pstmnt.setObject(i, object, OracleType.JSON);
		} else {
			pstmnt.setString(i, StringEscapeUtils.unescapeJava(fieldValue.toString()));
		}
	}

	public static List<String> getParamNames(String sqlScript, String type){
		List<String> paramNames = new ArrayList<>();
		Matcher matcher = Pattern.compile(PARAM_REGEX).matcher(sqlScript);
		while (matcher.find()) {
			String curParamName = matcher.group();
			int startingIndex = matcher.start();
			if(startingIndex > 0 && sqlScript.charAt(startingIndex) == '$' && sqlScript.charAt(startingIndex - 1) == '\\') {
				// escaping this parameter as it was preceded by backslash
				continue;
			}
			curParamName = curParamName.replace("'", "");
			curParamName = curParamName.replace("\"", "");
			curParamName = curParamName.replace("$", "");
			if(type.equalsIgnoreCase("standard") &&
					(curParamName.equalsIgnoreCase("where") || curParamName.equalsIgnoreCase("orderBy"))) {
				paramNames.add(curParamName.toUpperCase());
			} else {
				paramNames.add(curParamName);
			}
		}
		paramNames.sort((a, b) -> b.length() - a.length());
		return paramNames;
	}

	/**
	 * Sets the Interger value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setIntegerDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			BigDecimal num = new BigDecimal(value);
			bstmnt.setBigDecimal(i, num);
		} else {
			bstmnt.setNull(i, Types.INTEGER);
		}
	}

	/**
	 * Sets the Date value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDateDataType(String value, PreparedStatement bstmnt,
			int i, String databaseName) throws SQLException {
		if (value != null && !value.equals("null")) {
			if (DatabaseConnectorConstants.ORACLE.equals(databaseName)) {
				bstmnt.setString(i, value);
			} else {
				try {
					bstmnt.setDate(i, Date.valueOf(value));
				}catch(IllegalArgumentException e) {
					throw new IllegalArgumentException(DatabaseConnectorConstants.INVALID_ERROR +e);
				}
			}
		} else {
			bstmnt.setNull(i, Types.DATE);
		}
	}

	/**
	 * Sets the String value
	 * @param value
	 * @param bstmnt
	 * @param parameterIndex
	 * @throws SQLException
	 */
	private static void setStringDataType(String value, PreparedStatement bstmnt, int parameterIndex) throws SQLException {
		if (value != null && !value.equals("null")) {
			bstmnt.setString(parameterIndex, value);
		} else {
			bstmnt.setNull(parameterIndex, Types.VARCHAR);
		}
	}

	/**
	 * Sets the JSON value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setJSONDataType(String value, PreparedStatement bstmnt,
										int i, String databaseName) throws SQLException {
		if (value != null && !value.equals("null")) {
			extractJson(value, bstmnt, i, databaseName);
		} else {
			bstmnt.setNull(i, Types.NULL);
		}
	}

	/**
	 * Sets the Time value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimeDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			bstmnt.setTime(i, Time.valueOf(value));
		} else {
			bstmnt.setNull(i, Types.TIME);
		}
	}

	/**
	 * Sets the Boolean value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setBooleanDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			Boolean flag = Boolean.valueOf(value);
			bstmnt.setBoolean(i, flag);
		} else {
			bstmnt.setNull(i, Types.BOOLEAN);
		}
	}

	/**
	 * Sets the Long value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setLongDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			bstmnt.setLong(i, Long.parseLong(value));
		} else {
			bstmnt.setNull(i, Types.BIGINT);
		}
	}

	/**
	 * Sets the Float value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setFloatDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			bstmnt.setFloat(i, Float.parseFloat(value));
		} else {
			bstmnt.setNull(i, Types.FLOAT);
		}
	}

	/**
	 * Sets the Double value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setDoubleDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			BigDecimal num = new BigDecimal(value);
			bstmnt.setBigDecimal(i, num);
		} else {
			bstmnt.setNull(i, Types.DECIMAL);
		}
	}

	/**
	 * Sets the BLOB value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 * @throws IOException
	 */
	private static void setBLOBDataType(String value, PreparedStatement bstmnt,
			int i, String databaseName) throws SQLException, IOException {
		if ((value != null) && !"null".equals(value)) {
			try (InputStream stream = new ByteArrayInputStream(value.getBytes());) {
				if (DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) {
					bstmnt.setBinaryStream(i, stream);
				} else {
					bstmnt.setBlob(i, stream);
				}
			}
		} else {
			bstmnt.setNull(i, Types.BLOB);
		}
	}

	/**
	 * Sets the Timestamp value
	 * @param value
	 * @param bstmnt
	 * @param i
	 * @throws SQLException
	 */
	private static void setTimestampDataType(String value, PreparedStatement bstmnt, int i) throws SQLException {
		if (value != null && !value.equals("null")) {
			bstmnt.setTimestamp(i, Timestamp.valueOf(value));
		} else {
			bstmnt.setNull(i, Types.TIMESTAMP);
		}
	}

	/**
	 * convert readTimeout from milliseconds to the nearest second
	 * @param readTimeout in milliseconds
	 * @return readTimeout in seconds
	 */
	public static int convertReadTimeoutToSeconds(int readTimeout) {
		if(readTimeout == 0) {
			return 0;
		}
		else if(readTimeout<THOUSAND_MILLISECONDS) {
			return 1;
		} else {
			float seconds = readTimeout /THOUSAND_MILLISECONDS_FLOAT;
			return Math.round(seconds);
		}
	}

	/**
	 * Validate column name.
	 *
	 * @param sqlConnection the _sqlConnection
	 * @param key the key
	 * @return true, if successful
	 * @throws SQLException the SQL exception
	 */
	public static boolean doesColumnExistInTable(Connection sqlConnection, String key,
			String objectTypeId) throws SQLException {
		boolean flag = true;
		DatabaseMetaData md = sqlConnection.getMetaData();
		String databaseName = md.getDatabaseProductName();
		try (ResultSet resultSet1 = md.getColumns(null, null,
				QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId,databaseName), key);)  {
			if(resultSet1.next()) {
				flag = true;
			}else {
				flag = false;
			}
		}
		return flag;
	}

	/**
	 * Validates if a given column exists in the table
	 *
	 * @param key         Column name to validate
	 * @param columnNames Existing set of column names (can be null)
	 * @return boolean indicating if column exists
	 * @throws SQLException if database error occurs
	 */
	public static boolean doesColumnExistInTable(String key, Set<String> columnNames) {

		if (columnNames == null || columnNames.isEmpty()) {
			return false;
		}
		return columnNames.contains(key);
	}

	/**
	 * Retrieves all column names for a given table
	 *
	 * @param sqlConnection Database connection
	 * @param objectTypeId  Table name
	 * @return Set containing all column names
	 * @throws SQLException if database error occurs
	 */
	public static Set<String> getTableColumnsAsSet(String catalog, String schema, Connection sqlConnection,
			String objectTypeId) throws SQLException {

		if (sqlConnection == null || objectTypeId == null) {
			return new LinkedHashSet<>();
		}

		Set<String> columnNames = new LinkedHashSet<>();
		DatabaseMetaData metaData = sqlConnection.getMetaData();
		String databaseName = metaData.getDatabaseProductName();
		String sanitizedTableName = QueryBuilderUtil.checkSpecialCharacterInDb(objectTypeId, databaseName);
		try (ResultSet columnResult = metaData.getColumns(catalog, schema, sanitizedTableName, null)) {
			while (columnResult.next()) {
				String columnName = columnResult.getString(DatabaseConnectorConstants.COLUMN_NAME);
				if (columnName != null) {
					columnNames.add(columnName);
				}
			}
			return columnNames;
		}
	}

	/**
	 * This method will get the LinkedHashSet of column names from the cookie string.
	 *
	 * @param cookieString
	 * @return LinkedHashSet of column names or empty Set in case of any exception
	 */
	public static Set<String> extractColumnSetFromCookie(String cookieString) {
		// return an emptySet if cookieString is null or empty
		if (StringUtil.isBlank(cookieString)) {
			return Collections.emptySet();
		}
		try (JsonParser parser = DBv2JsonUtil.getObjectMapper().createParser(cookieString)) {
			// Move past the initial starting object
			parser.nextToken();
			while (!parser.isClosed()) {
				JsonToken token = parser.nextToken();

				if (token != JsonToken.FIELD_NAME || !DatabaseConnectorConstants.COLUMN_NAMES_KEY.equals(
						parser.getCurrentName())) {
					// skip until we get to "columnNames" field
					parser.skipChildren();
					continue;
				}
				// "columnNames" field is found, so now we move to the start of the array
				parser.nextToken();
				if (!parser.isExpectedStartArrayToken()) {
					// If we have reached here, that means the cookie is likely formatted wrong
					// no need to continue parsing
					break;
				}
				Set<String> columnSet = new LinkedHashSet<>();
				while (parser.nextToken() != JsonToken.END_ARRAY) {
					columnSet.add(parser.getValueAsString());
				}
				// Break out of the loop once the column names are processed
				return columnSet;
			}
			return Collections.emptySet();
		} catch (IOException e) {
			LOG.log(Level.FINE, e, () -> String.format("Error parsing cookie string: %s", e.getMessage()));
			return Collections.emptySet();
		}
	}

	/**
	 * Retrieves column names for a specified table either from a cached cookie or through a database query.
	 * The method first attempts to extract column names from the cookie. If no columns are found in the cookie,
	 * it falls back to querying the database directly.
	 *
	 * @param context       The operation context containing cookie information and object type details
	 * @param sqlConnection The active database connection
	 * @param schema        The database schema name
	 * @return A Set of column names for the specified table
	 * @throws SQLException If there is an error accessing the database
	 */
	public static Set<String> retrieveTableColumns(OperationContext context, Connection sqlConnection, String schema)
			throws SQLException {
		String cookie = context.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT);
		java.util.Set<String> columnNames = QueryBuilderUtil.extractColumnSetFromCookie(cookie);
		columnNames = columnNames.isEmpty() ? QueryBuilderUtil.getTableColumnsAsSet(sqlConnection.getCatalog(), schema,
				sqlConnection, context.getObjectTypeId()) : columnNames;
		return columnNames;
	}

	/**
	 * Gets the schema name for the MSSQL DB as MSSQL JDBC driver doesn't support set schema in connection
	 * @param con
	 * @param schemaName
	 * @param schemaNameFromConnection
	 * @return schemaName
	 */
	public static String getSchemaNameForMSSQL(Connection con, String schemaName, String schemaNameFromConnection){

		try {
			if (schemaName != null && schemaName.isEmpty() && !schemaNameFromConnection.isEmpty()) {
				return schemaNameFromConnection;
			}
			if (con.getMetaData().getDatabaseProductName().equals(DatabaseConnectorConstants.MSSQL)
					&& !StringUtil.isEmpty(schemaName)) {
				return schemaName;
			}
			return con.getSchema();
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}

	/**
	 * Gets the schema name from the connection based on the Database
	 * @param database
	 * @param con
	 * @param schemaName
	 * @param schemaNameFromConnection
	 * @return
	 */
	public static String getSchemaFromConnection(String database, Connection con,
			String schemaName, String schemaNameFromConnection) {
		if(database.equals(DatabaseConnectorConstants.MSSQL)) {
			return QueryBuilderUtil.getSchemaNameForMSSQL(con, schemaName, schemaNameFromConnection);
		} else {
			try {
				return con.getSchema();
			} catch (SQLException e) {
				throw new ConnectorException(e);
			}
		}
	}

	/**
	 * Utility class to unescape special characters such as single (\') quotes, double quotes (\"), backslash (\\)
	 * etc. when using these with stored procedure input parameters.
	 *
	 * @param node - JSON Node to be converted into text
	 * @return the input node as text with unescaped Java literals or returns null if JSON Node is null
	 */
	public static String unescapeEscapedStringFrom(JsonNode node) {
		if (node != null) {
			return StringEscapeUtils.unescapeJava(node.asText());
		}
		return null;
	}

	/**
	 * This method is to replace '*' with SQL wildcard '%' if the given pattern is neither empty nor null
	 * @param pattern Store Procedure Pattern
	 * @return updated string
	 */
	public static String replaceWithSqlWildCards(String pattern) {
		return StringUtil.fastReplace(StringUtil.trimToNull(pattern), "*", "%");
	}

	/**
	 * This method will check if the property is configured as DOP : Dynamic Operation Property , if yes then it will
	 * override the property value with DOP configured value else return the same value which is passed as an argument
	 * @param objectData
	 * @param maxRowConfiguredInOp : Property value configured at operation level
	 * @return maxRow configured as DOP . If Property is not configured as DOP , it will return the same maxRow passed
	 * as an argument i.e. maxRowConfiguredInOp
	 */
	public static Long getMaxRowIfConfigureAsDOP(ObjectData objectData, Long maxRowConfiguredInOp) {
		if (objectData == null) {
			return maxRowConfiguredInOp;
		}
		String maxRowProperty = objectData.getDynamicOperationProperties()
				.getProperty(DatabaseConnectorConstants.MAX_ROWS);
		if (StringUtil.isBlank(maxRowProperty)) {
			return maxRowConfiguredInOp;
		}
		try {
			return Long.parseLong(maxRowProperty);
		} catch (NumberFormatException exception) {
			objectData.getLogger().warning(() -> DatabaseConnectorConstants.MAX_ROW_NOT_PARSABLE + maxRowProperty);
			return maxRowConfiguredInOp;
		}
	}

	/**
	 * Utility method to remove the questions from build query which is not passed from json node.
	 *
	 * @param query the String builder query
	 * @param numberToRemove total number of remove questions from query
	 */
	public static void removeQuestionMarks(StringBuilder query, int numberToRemove) {
		if (query == null || numberToRemove < 1) {
			return;
		}
		int questionMarkStart = query.indexOf("?");
		if (questionMarkStart < 0) {
			return;
		}
		int questionMarkEnd = query.lastIndexOf("?");
		do {
			query.deleteCharAt(questionMarkEnd--);
			while (questionMarkEnd >= questionMarkStart && query.charAt(questionMarkEnd) != '?') {
				query.deleteCharAt(questionMarkEnd--);
			}
		} while (questionMarkEnd >= questionMarkStart && --numberToRemove > 0);
	}

	/**
	 * Validates that the data type mappings are not empty. If the `dataTypes` map is empty,
	 * it throws a SQLException indicating that the specified table does not exist in the schema.
	 *
	 * @param dataTypes    A map containing the data type mappings
	 * @param objectTypeId The ID of the object type (table name)
	 * @throws SQLException If the `dataTypes` map is empty
	 */
	public static void validateDataTypeMappings(Map<String, String> dataTypes, String objectTypeId)
			throws SQLException {
		if (dataTypes.isEmpty()) {
			throw new SQLException("The specified table \"" + objectTypeId + DatabaseConnectorConstants.DOUBLE_QUOTE
					+ " does not exist");
		}
	}
}