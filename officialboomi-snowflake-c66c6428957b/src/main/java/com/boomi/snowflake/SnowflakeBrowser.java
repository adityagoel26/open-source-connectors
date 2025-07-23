// Copyright (c) 2025 Boomi, LP
package com.boomi.snowflake;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;

import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.snowflake.override.ImportableField;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.stages.AmazonWebServicesHandler;
import com.boomi.snowflake.util.TableDefaultAndMetaDataObject;
import com.boomi.snowflake.util.SnowflakeDataTypeConstants;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Deque;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.apache.commons.codec.digest.DigestUtils;
import net.snowflake.client.jdbc.internal.net.minidev.json.JSONObject;

/**
 * The Class SnowflakeBrowser.
 */
public class SnowflakeBrowser extends BaseBrowser implements ConnectionTester {

	/** The Constant LOG. */
	private static final Logger LOG = LogUtil.getLogger(SnowflakeBrowser.class);
	/** The Constant SQL_COMMAND_GET_PROCEDURES_DATA. */
	private static final String SQL_COMMAND_GET_PROCEDURES_DATA = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA"
			+ ",PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from ";
	/** The Constant SQL_COMMAND_GET_PROCEDURES_FROM. */
	private static final String SQL_COMMAND_GET_PROCEDURES_FROM = "\"INFORMATION_SCHEMA\".\"PROCEDURES\" ";
	/** The Constant SQL_COMMAND_FILTER_PROCEDURES. */
	private static final String SQL_COMMAND_FILTER_PROCEDURES = "where procedure_schema = '";
	/** The Constant SQL_COMMAND_STREAMS. */
	private static final String SQL_COMMAND_STREAMS = "show streams";
	/** The Constant SQL_COMMAND_SELECT_ALL. */
	private static final String SQL_COMMAND_SELECT_ALL = "select * from ";
	/** The Constant SNOWSQL_DEFAULT_PROFILE_NAME. */
	private static final String SNOWSQL_DEFAULT_PROFILE_NAME = "Dummy_Profile";
	/** The Constant SNOWSQL_DEFAULT_PROFILE_ID. */
	private static final String SNOWSQL_DEFAULT_PROFILE_ID = "f9d57e4a-7740-479d-8110-b3f0ab2bd3dc";
	/** The Constant SNOWFLAKE_TYPE. */
	private static final String SNOWFLAKE_TYPE = "type";
	/** The Constant SNOWFLAKE_FORMAT. */
	private static final String SNOWFLAKE_FORMAT = "format";
	/** The Constant SNOWFLAKE_SCHEMA. */
	private static final String SNOWFLAKE_SCHEMA = "schema";
	/** The Constant SCHEMA_NAME. */
	private static final String SCHEMA_NAME = "http://json-schema.org/schema";
	/** The Constant SNOWFLAKE_SCHEMA. */
	private static final String SNOWFLAKE_SCHEMA_JSON = "$schema";
	/** The Constant SNOWFLAKE_OBJECT. */
	private static final String SNOWFLAKE_OBJECT = "object";
	/** The Constant SNOWFLAKE_PROPERTIES. */
	private static final String SNOWFLAKE_PROPERTIES = "properties";
	/** The Constant SNOWFLAKE_UPDATE_COUNT. */
	private static final String SNOWFLAKE_UPDATE_COUNT = "Update_Count";
	/** The Constant SNOWFLAKE_ARRAY. */
	private static final String SNOWFLAKE_ARRAY = "array";
	/** The Constant SNOWFLAKE_ITEMS. */
	private static final String SNOWFLAKE_ITEMS = "items";
	/** The Constant SNOWFLAKE_TITLE. */
	private static final String SNOWFLAKE_TITLE = "title";
	/** The Constant SNOWFLAKE_ARRAY_NAME. */
	private static final String SNOWFLAKE_ARRAY_NAME = "CreateArray";
	/** The Constant SNOWFLAKE_ARRAY_NAME. */
	private static final String SNOWFLAKE_ARRAY_NAME_QUERY = "BatchArray";
	/** The Constant SNOWFLAKE_OBJECT_NAME. */
	private static final String SNOWFLAKE_OBJECT_NAME = "CreateObject";
	/** The Constant SNOWFLAKE_BATCHING. */
	private static final String SNOWFLAKE_BATCHING = "documentBatching";
	/** The Constant DB_CONNECTION_ERROR. */
	private static final String DB_CONNECTION_ERROR = "Unable to connect: Unexpected database (and/or) schema name";
	/** The Constant STR_TABLE. */
	private static final String STR_TABLE = "TABLE";
	/** The Constant BEGIN_INDEX. */
	private static final int BEGIN_INDEX = 1;
	/** The Constant TWO. */
	private static final int TWO = 2;
	/** The Constant THREE. */
	private static final int THREE = 3;
	/** The Constant FOUR. */
	private static final int FOUR = 4;
	/** The Constant FIVE. */
	private static final int FIVE = 5;
	/** The Constant SIX. */
	private static final int SIX = 6;
	/** The Constant TWO_HUNDRED. */
	private static final int TWO_HUNDRED = 200;
	/** The Constant TWO_HUNDRED_FIFTY_FIVE. */
	private static final int TWO_HUNDRED_FIFTY_FIVE = 255;
	/** The overridden schema. If not overridden then use connection schema */
	private static String OVERRIDDEN_SCHEMA;
	/** The overridden database. If not overridden then use connection db */
	private static String OVERRIDDEN_DB;

	private String _defaultValueMapAsString;

	/**
	 * Sets the overridden schema for the Snowflake connection.
	 *
	 * @param schema The schema name to override the default schema.
	 */
	public static void setOverriddenSchema(String schema) {
		OVERRIDDEN_SCHEMA = schema;
	}

	/**
	 * Sets the overridden db for the Snowflake connection.
	 *
	 * @param db The schema name to override the default db.
	 *
	 */
	public static void setOverriddenDb(String db) {
		OVERRIDDEN_DB = db;
	}

	/**
	 * Instantiates a SnowflakeBrowser.
	 * @param conn
	 * 			the Snowflake Connection
	 * */
	@SuppressWarnings("unchecked")
	protected SnowflakeBrowser(SnowflakeConnection conn) {
		super(conn);
	}

	/**
	 * Gets the Object Properties
	 * @param objectTypeId Object Type ID
	 * @param connection Connection Parameters
	 * @return JSONObject
	 */
	public static JSONObject getObjectProperties(String objectTypeId, Connection connection) {
		return getObjectProperties(objectTypeId, connection, null);
	}

	/**
	 * Gets the Object Properties
	 * @param objectTypeId Object Type ID
	 * @param connection Connection Parameters
	 * @param metadata SortedMap list of columns and datatypes of tables
	 * @return JSONObject
	 */
	public static JSONObject getObjectProperties(String objectTypeId, Connection connection, SortedMap<String,
				String> metadata) {
		JSONObject definition = new JSONObject();
		String[] objectData = objectTypeId.split("\\.");
		if (objectData.length == FIVE) {
			// remove braces
			String signature = objectData[THREE].replace("(", "").replace(")", "");
			String[] arguments = signature.split("\\,");

			for (int i = 0; i < arguments.length; i++) {
				String[] arg = arguments[i].trim().split("\\s+");
				if (arg != null && arg.length > 1) {
					definition.appendField(arguments[i].trim(), getProfileDataTypes(arg[1].trim()));
				} else {
					definition.appendField(arguments[i].trim(), new JSONObject().appendField("type", "string"));
				}
			}
		} else if (!objectData[0].equals(SNOWSQL_DEFAULT_PROFILE_ID)) {
			addObjectPropertiesFromDb(objectTypeId, connection, definition, objectData, metadata);
		}
		return definition;
	}

	/**
	 * Get snowflake table name from objectTypeId.
	 *
	 * @param tableName the objectTypeId
	 * @return table name
	 */
	public static String getModifiedQuery(String tableName) {
		String sanitizedTableName = tableName.replaceAll("[^a-zA-Z0-9_]", "");
		return SQL_COMMAND_SELECT_ALL +sanitizedTableName + "";
	}

	/**
	 * Adds object properties from Snowflake.
	 *
	 * @param objectTypeId the object type ID
	 * @param connection   the connection parameters
	 * @param definition   the JSON object to add properties to
	 * @param objectData   the object data
	 */
	private static void addObjectPropertiesFromDb(String objectTypeId, Connection connection, JSONObject definition,
			String[] objectData, SortedMap<String, String> metadata) {

		if(!CollectionUtil.isEmpty(metadata)) {
			Iterator<String> itr = metadata.keySet().iterator();
			while(itr.hasNext()) {
				String columnName = itr.next();
				String columnDataType = metadata.get(columnName);
				definition.appendField(columnName, getProfileDataTypes(columnDataType));
			}
		} else {
			try (ResultSet columnsRS = connection.getMetaData()
					.getColumns(OVERRIDDEN_DB, OVERRIDDEN_SCHEMA,
							objectData[0].replace("\"", ""), "%")) {
				if (columnsRS.next()) {
					// then it has to be a table
					do {
						definition.appendField(columnsRS.getString(FOUR), getProfileDataTypes(columnsRS.getString(SIX)));
					} while (columnsRS.next());
				} else {
					// else it has to be a stream
					String query = getModifiedQuery(objectTypeId);
					try (PreparedStatement statement = connection.prepareStatement(query);
						 ResultSet streamRS = statement.executeQuery()) {
						for (int i = 1; i <= streamRS.getMetaData().getColumnCount(); i++) {
							definition.appendField(streamRS.getMetaData().getColumnName(i),
									getProfileDataTypes(streamRS.getMetaData().getColumnTypeName(i)));
						}
					}
				}
			} catch (SQLException e) {
				throw new ConnectorException(DB_CONNECTION_ERROR, e);
			}
		}
	}

	/**
	 * this function is called when importing profile
	 * @param objectTypeId Object Type ID
	 * @param roles Collection of Object Roles
	 * @return ObjectDefinitions
	 */
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {
		LOG.entering(this.getClass().getCanonicalName(), "getObjectDefinitions()");
		String[] objectData = objectTypeId.split("\\.");
		// set object definitions to JSON
		ObjectDefinition def = new ObjectDefinition();
		boolean isBatching = false;
		def.setInputType(ContentType.JSON);
		def.setOutputType(ContentType.JSON);
		def.setElementName("");
		if ((getContext().getOperationType() == OperationType.GET || getContext().getOperationType() == OperationType.QUERY)
				&& (getContext().getOperationProperties() != null && !getContext().getOperationProperties().isEmpty())) {
			JSONObject jsonCookie = new JSONObject();
			isBatching = getContext().getOperationProperties().getBooleanProperty(SNOWFLAKE_BATCHING, false);
			jsonCookie.appendField(SNOWFLAKE_BATCHING, isBatching);
			def.withCookie(jsonCookie.toJSONString());
		}
		
		// constructing JSON profile
		JSONObject json = getJsonProfile(objectTypeId, objectData, isBatching);
		def.setJsonSchema(json.toString());
		ObjectDefinitions defs = new ObjectDefinitions();
		defs.getDefinitions().add(def);
		if (getContext().getOperationType() == OperationType.CREATE) {
			ObjectDefinition resDef = createResponseProfileForCreate();
			defs.getDefinitions().add(resDef);
			def.setCookie(_defaultValueMapAsString);
		}
		displayConnectionFields(getContext().getOperationProperties(),defs);
		return defs;
	}

	private JSONObject getJsonProfile(String objectTypeId, String[] objectData, boolean isBatching) {
		JSONObject json = new JSONObject();
		JSONObject jsonArrayObj = new JSONObject();
		json.appendField(SNOWFLAKE_SCHEMA_JSON, SCHEMA_NAME);
		if ((getContext().getOperationType() == OperationType.GET || getContext().getOperationType() ==
				OperationType.QUERY) && isBatching) {
			json.appendField(SNOWFLAKE_TYPE, SNOWFLAKE_ARRAY);
			json.appendField(SNOWFLAKE_TITLE, SNOWFLAKE_ARRAY_NAME_QUERY);
			jsonArrayObj.appendField(SNOWFLAKE_TYPE, SNOWFLAKE_OBJECT);
			jsonArrayObj.appendField(SNOWFLAKE_TITLE, objectData[0].equals(SNOWSQL_DEFAULT_PROFILE_ID) ?
					SNOWSQL_DEFAULT_PROFILE_NAME : objectData[0].substring(0,
					Math.min(TWO_HUNDRED_FIFTY_FIVE, objectData[0].length())).replace("\"", ""));
		} else {
			json.appendField(SNOWFLAKE_TYPE, SNOWFLAKE_OBJECT);
			json.appendField(SNOWFLAKE_TITLE, objectData[0].equals(SNOWSQL_DEFAULT_PROFILE_ID) ?
					SNOWSQL_DEFAULT_PROFILE_NAME : objectData[0].substring(0, Math.min(TWO_HUNDRED_FIFTY_FIVE,
					objectData[0].length())).replace("\"", ""));
		}
		// add definitions to object definition array
		createObjectDefinitionArray(objectTypeId, isBatching, json, jsonArrayObj);
		return json;
	}

	private void createObjectDefinitionArray(String objectTypeId, boolean isBatching, JSONObject json,
			JSONObject jsonArrayObj) {
		try (Connection connection = getConnection().createJdbcConnection()) {
			if ((getContext().getOperationType() == OperationType.GET ||
					getContext().getOperationType() == OperationType.QUERY) && isBatching) {
				jsonArrayObj.appendField(SNOWFLAKE_PROPERTIES, getObjectProperties(objectTypeId, connection));
				json.appendField(SNOWFLAKE_ITEMS, jsonArrayObj);
			}
			else {
				if (getContext().getOperationType() == OperationType.CREATE){
					setCookieData(objectTypeId, connection);
				}
				json.appendField(SNOWFLAKE_PROPERTIES, getObjectProperties(objectTypeId, connection));
			}
		} catch (SQLException | JsonProcessingException e) {
			throw new ConnectorException("Unable to connect to snowflake", e);
		}
	}

	/**
	 * Fetches and processes metadata, default values, and table metadata for the specified object type,
	 * then stores them as JSON strings in class variables.
	 *
	 * @param objectTypeId The ID of the object type.
	 * @param connection   The database connection to use.
	 *
	 * @throws ConnectorException If a JSON processing error occurs.
	 */
    public void setCookieData(String objectTypeId, Connection connection)
            throws SQLException, JsonProcessingException {
//		DB and Schema should not be null or Empty
		if (StringUtil.isNotBlank(OVERRIDDEN_DB) && StringUtil.isNotBlank(OVERRIDDEN_SCHEMA)) {
			SnowflakeConnection snowflakeConnection = getConnection();
			TableDefaultAndMetaDataObject tableDefaultAndMetaDataObject = new TableDefaultAndMetaDataObject();
			Deque<RuntimeException> exceptionStack = new LinkedList<>();
			tableDefaultAndMetaDataObject.setMetaDataValues(SnowflakeOperationUtil.getTableMetadata(objectTypeId,connection,
					OVERRIDDEN_DB, OVERRIDDEN_SCHEMA));
			tableDefaultAndMetaDataObject.setTableMetaDataValues(SnowflakeOperationUtil.getTableMetadataForCreate(objectTypeId,
					connection,OVERRIDDEN_DB,OVERRIDDEN_SCHEMA));
			tableDefaultAndMetaDataObject.setDefaultValues(
					SnowflakeOperationUtil.getDefaultsValues(objectTypeId, connection, null));
			SortedMap<String, TableDefaultAndMetaDataObject> tableDefaultAndMetaDataValues = new TreeMap<>();
			String key = DigestUtils.sha256Hex(connection.getMetaData().getURL() +
					"|" + OVERRIDDEN_DB + "|" + OVERRIDDEN_SCHEMA);
			tableDefaultAndMetaDataValues.put(key, tableDefaultAndMetaDataObject);
			_defaultValueMapAsString  = SnowflakeOperationUtil.getObjectMapper().
					writeValueAsString(tableDefaultAndMetaDataValues);
			if (ConnectionOverrideUtil.isConnectionSettingsOverride(
					getContext().getOperationProperties()) && getContext().getConnectionProperties().getBooleanProperty(
					SnowflakeOverrideConstants.PROP_ENABLE_POOLING, false)) {
				ConnectionOverrideUtil.resetConnection(snowflakeConnection, connection, exceptionStack);
			}
		}
	}

	/**
	 * fills object types with stored procedures given database name
	 * 
	 * @param connection    Snowflake connection
	 * @param returnedTypes returned types that contains table and stored procedures
	 * @param dbName        database name that needs to be explored and retrieve
	 *                      stored procedures meta data from it
	 * @param schemaName	Schema Name that needs to be retrieved.
	 * @throws SQLException
	 */
	private void fillSPObjectType(Connection connection, ObjectTypes returnedTypes, String dbName, String schemaName)
			throws SQLException {
		LOG.entering(this.getClass().getCanonicalName(), "fillSPObjectType()");
		String query = getModifiedFillSPObjectType(dbName,schemaName);
		try (PreparedStatement statement = connection.prepareStatement(query);
			 ResultSet procedureResultSet = statement.executeQuery();) {
			while (procedureResultSet.next()) {
				ObjectType current = new ObjectType();
				String value = procedureResultSet.getString(1) + "." +
						procedureResultSet.getString(TWO) + "." + procedureResultSet.getString(THREE)
						+ "." + procedureResultSet.getString(FOUR) + "." +
						procedureResultSet.getString(FIVE);
				String procedureID = "\"" + procedureResultSet.getString(1) + "\"" + "." + "\"" +
						procedureResultSet.getString(TWO) + "\"" + "." + "\"" +
						procedureResultSet.getString(THREE) + "\"" + "." +
						procedureResultSet.getString(FOUR) + "." + procedureResultSet.getString(FIVE);
				
				current.setId(procedureID);
				current.setLabel(value.substring(0, Math.min(TWO_HUNDRED, value.length())));
				returnedTypes.getTypes().add(current);
			}
		}
	}

	/**
	 * Constructs a SQL query string to retrieve stored procedures from a specified database
	 * and schema, with optional schema formatting.
	 *
	 * @param dbName    the name of the database, enclosed in double quotes if included in the query;
	 *                  may be {@code null} for no database-specific query.
	 * @param schemaName the name of the schema, optionally enclosed in double quotes;
	 *                   may be {@code null} if no schema-specific query is needed.
	 *                   If the schema name starts and ends with double quotes, they are removed
	 *                   before constructing the query.
	 * @return a SQL query string to fetch stored procedures, including any database and schema
	 *         information provided, formatted according to Snowflake conventions. If neither
	 *         database nor schema is specified, the query will retrieve procedures from the default
	 *         scope.
	 */

	private static String getModifiedFillSPObjectType(String dbName, String schemaName) {
			if ( schemaName != null && (schemaName.startsWith(SnowflakeOverrideConstants.DOUBLE_QUOTE) &&
					schemaName.endsWith(SnowflakeOverrideConstants.DOUBLE_QUOTE))) {
				schemaName = schemaName.substring(BEGIN_INDEX, schemaName.length() - BEGIN_INDEX) ;
				}
		return SQL_COMMAND_GET_PROCEDURES_DATA + (dbName == null ? StringUtil.EMPTY_STRING
				: (SnowflakeOverrideConstants.DOUBLE_QUOTE + dbName + SnowflakeOverrideConstants.DOUBLE_QUOTE
						+ SnowflakeOverrideConstants.DOT)) + SQL_COMMAND_GET_PROCEDURES_FROM + (schemaName != null ? (
				SQL_COMMAND_FILTER_PROCEDURES + schemaName
						+ SnowflakeOverrideConstants.SINGLE_QUOTE) : StringUtil.EMPTY_STRING);
	}

	/**
	 * get all objects filtered by schema name and database name.
	 * 
	 * @param types      object types that needs to be retrieved.
	 * @param streamType when true streams is going to be included
	 * @return object types containing tables only.
	 */
	private ObjectTypes getObjects(String[] types, boolean streamType) {
		LOG.entering(this.getClass().getCanonicalName(), "getTables()");
		String inputSchema = getContext().getConnectionProperties().getProperty(SNOWFLAKE_SCHEMA, null);
		inputSchema = overrideSchemaValue(inputSchema);
		inputSchema = ((inputSchema != null) && (inputSchema.trim().length() != 0)) ? inputSchema.trim() : null;
		return getObjectTypesFromDb(types, streamType, inputSchema);
	}

	private ObjectTypes getObjectTypesFromDb(String[] types, boolean streamType, String inputSchema) {
		ObjectTypes returnedTypes = new ObjectTypes();
		SnowflakeConnection snowflakeConnection = getConnection();
		Connection connection = null;
		try {
			connection = snowflakeConnection.createJdbcConnection();
			setOverriddenDb(overrideCatalogDetails(connection));
			if(null !=OVERRIDDEN_DB && !OVERRIDDEN_DB.isEmpty() && OVERRIDDEN_DB.trim().isEmpty()){
				return returnedTypes;
			}
			setOverriddenSchema(inputSchema);
			ResultSet objectResultSet = connection.getMetaData().getTables(OVERRIDDEN_DB, OVERRIDDEN_SCHEMA ==
					null ? null : OVERRIDDEN_SCHEMA.replace("\"", ""), "%", null);
			populateTypesFromDb(types, returnedTypes, objectResultSet);
			if (streamType) {
				String filter = getStreamFilter(OVERRIDDEN_SCHEMA, OVERRIDDEN_DB);

				boolean dbProvided = OVERRIDDEN_DB != null;

				try (ResultSet dbResultSet = dbProvided ? null : connection.getMetaData().getCatalogs()) {
					if(dbProvided || (dbResultSet != null && dbResultSet.next())) {
						populateTypesFromStream(returnedTypes, connection, filter, dbProvided, dbResultSet);
					}
				}
			}
		} catch (Exception e) {
			throw new ConnectorException(DB_CONNECTION_ERROR, e);
		}finally {
			// This will take care of closing the connection
			closeAndResetConnectionIfRequired(snowflakeConnection,connection);
		}
		return returnedTypes;
	}

	/**
	 * override catalog by database name.
	 *
	 * @param connection Snowflake connection.
	 * @throws SQLException
	 * @Return catalog
	 */
	private String overrideCatalogDetails(Connection connection) throws SQLException {
		try {
			if(ConnectionOverrideUtil.isConnectionSettingsOverride(getContext().getOperationProperties())){
				String db = getContext().getOperationProperties().getProperty(SnowflakeOverrideConstants.DATABASE);
				if (null != db && db.isEmpty()) {
					return null;
				}
				else if(null != db && !db.isEmpty() && db.trim().isEmpty()){
					return db;
				}
				else {
					return ConnectionOverrideUtil.normalizeString(db);
				}
			}
			return connection.getCatalog();
		}
		catch(SQLException e)
		{
			LOG.log(Level.WARNING, "Unable to set the catalog for the connection. ", e);
			return SnowflakeOverrideConstants.BLANK_STRING;
		}

	}

	private static void populateTypesFromStream(ObjectTypes returnedTypes, Connection connection, String filter, boolean
			dbProvided, ResultSet dbResultSet) throws SQLException {
		do {
			if (!dbProvided) {
				try {
					connection.setCatalog(dbResultSet.getString(1));
				} catch (SQLException e) {
					LOG.log(Level.WARNING, "Unable to set the catalog for the connection. ", e);
					//if this database is view only, skip it
					if (dbResultSet.next()) {
						continue;
					}
					break;
				}
			}

			populateFromPreparedStatement(returnedTypes, connection, filter);
			if (dbProvided) {
				break;
			}
		} while (dbResultSet.next());
	}

	private static String getModifiedFilterQuery(String filter){
		return SQL_COMMAND_STREAMS + filter;
	}

	private static void populateFromPreparedStatement(ObjectTypes returnedTypes, Connection connection, String filter) {
		String query = getModifiedFilterQuery(filter);
		try (PreparedStatement statement = connection.prepareStatement(query);
			 ResultSet streamRS = statement.executeQuery()) {
			while (streamRS.next()) {
				ObjectType current = new ObjectType();
				String id = "" + streamRS.getString(TWO) + "";
				String label = streamRS.getString(THREE) + "." + streamRS.getString(FOUR) + "." +
						streamRS.getString(TWO)  + "\t(STREAM)";
				current.setId(id);
				current.setLabel(label.substring(0, Math.min(TWO_HUNDRED, label.length())));
				returnedTypes.getTypes().add(current);
			}

		}catch(SQLException e) {
			LOG.log(Level.WARNING, e, () -> String.format(
					"Possible causes for this error likely arise from  " +
							"database access error, invalid column index or closed result set: %s"
					, e.getMessage()));
			// if we don't have access to that schema then skip it
		}
	}

	/**
	 * Generates a filter string for Snowflake database/schema queries based on the provided input parameters.
	 *
	 * @param inputSchema The name of the schema to filter by. Can be null.
	 * @param catalog    The name of the database (catalog) to filter by. Can be null.
	 * @return          A formatted filter string based on the following rules:
	 *                  - If both catalog and schema are provided: returns " in schema \"catalog\".schema"
	 *                  - If only catalog is provided: returns " in database \"catalog\""
	 *                  - If only schema is provided: returns " in schema schema"
	 *                  - If both are null: returns empty string
	 */
	private static String getStreamFilter(String inputSchema, String catalog) {
		String filter = "";
		if (catalog != null && inputSchema != null) {
			filter += " in schema \"" + catalog + "\"." + inputSchema + "";
		} else if (catalog != null) {
			filter = " in database \"" + catalog + "\"";
		} else if (inputSchema != null) {
			filter += " in schema " + inputSchema;
		}
		return filter;
	}

	private static void populateTypesFromDb(String[] types, ObjectTypes returnedTypes, ResultSet objectResultSet)
			throws SQLException {
		// get all object names in the database
		// get object names
		while (objectResultSet.next()) {
			ObjectType current = new ObjectType();
			// dbName.schemaName.objectName
			if (Arrays.asList(types).contains(objectResultSet.getString(FOUR))) {
				String value = objectResultSet.getString(THREE);
				String tableID = "\"" + value + "\"";
				String label = objectResultSet.getString(1) + "." + objectResultSet.getString(TWO)
						+ "." + objectResultSet.getString(THREE) + "\t" + "(" +
						objectResultSet.getString(FOUR) + ")";
				current.setId(tableID);
				current.setLabel(label.substring(0, Math.min(TWO_HUNDRED, label.length())));
				returnedTypes.getTypes().add(current);
			}
		}
	}

	/**
	 * get all stored procedures filtered by schema name and database name
	 * 
	 * @return object types containing stored procedures only
	 */
	private ObjectTypes getStoredProcedures() {
		LOG.entering(this.getClass().getCanonicalName(), "storedProcedures()");
		ObjectTypes returnedTypes = new ObjectTypes();
		try (Connection connection = getConnection().createJdbcConnection()) {
			setOverriddenDb(overrideCatalogDetails(connection));
			setOverriddenSchema(overrideSchemaValue(
					getContext().getConnectionProperties().getProperty(SNOWFLAKE_SCHEMA)));
			//To handle empty,blank and null scenarios
			if(StringUtil.isBlank(OVERRIDDEN_DB) && StringUtil.isBlank(OVERRIDDEN_SCHEMA)){
				return returnedTypes;
			}
			if (OVERRIDDEN_DB == null) {
				try (ResultSet dbResultSet = connection.getMetaData().getCatalogs()) {
					while (dbResultSet.next()) {
						fillSPObjectType(connection, returnedTypes, dbResultSet.getString(1), OVERRIDDEN_SCHEMA);
					}
				}
			} else {
				fillSPObjectType(connection, returnedTypes, OVERRIDDEN_DB, OVERRIDDEN_SCHEMA);
			}
		} catch (SQLException e) {
			throw new ConnectorException(DB_CONNECTION_ERROR, e);
		}
		return returnedTypes;
	}

	/**
	 * get dummy object type for SnowSql operation
	 * 
	 * @return object type containing the dummy object
	 */
	private ObjectTypes getSnowSQLGeneratedProfile() {
		LOG.entering(this.getClass().getCanonicalName(), "getSnowSQLGeneratedProfile()");
		ObjectTypes returnedTypes = new ObjectTypes();
		ObjectType current = new ObjectType();
		current.setId(SNOWSQL_DEFAULT_PROFILE_ID);
		current.setLabel(SNOWSQL_DEFAULT_PROFILE_NAME);
		returnedTypes.getTypes().add(current);
		return returnedTypes;
	}

	/**
	 * this function is called when the user has to chose an object to perform
	 * operation on
	 * @return Object Types
	 */
	@Override
	public ObjectTypes getObjectTypes() {
		LOG.entering(this.getClass().getCanonicalName(), "getObjectTypes()");
		if (getContext().getCustomOperationType() == null) {
			if (getContext().getOperationType() == OperationType.QUERY
					|| getContext().getOperationType() == OperationType.GET) {
				return getObjects(new String[] { "EXTERNAL_TABLE", STR_TABLE, "VIEW" }, true);
			}
			return getObjects(new String[] { STR_TABLE }, false);
		}
		switch (getContext().getCustomOperationType()) {
		case "bulkLoad":
		case "copyIntoTable":
			return getObjects(new String[] { STR_TABLE }, false);
		case "bulkUnload":
		case "copyIntoLocation":
			return getObjects(new String[] { "EXTERNAL_TABLE", STR_TABLE, "VIEW" }, false);
		case "snowSQL":
		case "GET":
		case "PUT":
			return getSnowSQLGeneratedProfile();
		case "EXECUTE":
			return getStoredProcedures();
		default:
			return new ObjectTypes();
		}
	}

	/**
	 * Gets the Snowflake Connection
	 */
	@Override
	public SnowflakeConnection getConnection() {
		return (SnowflakeConnection) super.getConnection();
	}

	/**
	 * this function is called when test connection button is pressed
	 */
	@Override
	public void testConnection() {
		LOG.entering(this.getClass().getCanonicalName(), "testConnection()");
		// Create JDBC connection then close it
		try (Connection con = getConnection().createJdbcConnection()) {
			// no op
		} catch (Exception e) {
			throw new ConnectorException("Unable to connect to Snowflake", e);
		}

		String awsAccessKey = getConnection().getAWSAccessKey();
		String awsSecretKey = getConnection().getAWSSecret();
		try {
			if (awsAccessKey.length() > 0) {
				AmazonWebServicesHandler.testConnectionCredentials(awsAccessKey, awsSecretKey);
			}
		} catch (Exception e) {
			throw new ConnectorException("AWS connection failed", e);
		}
	}
	
	/**
	 * get object data type for the profile
	 * 
	 * @dataType data type of the snowflake DB
	 * @return data type of the Boomi fields
	 */
	private static JSONObject getProfileDataTypes(String dataType) {
		switch (dataType){
		
		case SnowflakeDataTypeConstants.SNOWFLAKE_BOOLEANTYPE:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_BOOLEAN);
		
		case SnowflakeDataTypeConstants.SNOWFLAKE_DATETYPE:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_CHARACTER)
					.appendField(SNOWFLAKE_FORMAT, SnowflakeDataTypeConstants.SNOWFLAKE_DATE);
		
		case SnowflakeDataTypeConstants.SNOWFLAKE_TIMETYPE:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_CHARACTER)
					.appendField(SNOWFLAKE_FORMAT, SnowflakeDataTypeConstants.SNOWFLAKE_TIME);
		
		case SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_LTZ:
		case SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_NTZ:
		case SnowflakeDataTypeConstants.SNOWFLAKE_TIMESTAMP_TZ:
		case SnowflakeDataTypeConstants.SNOWFLAKE_DATETIMETYPE:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_CHARACTER)
					.appendField(SNOWFLAKE_FORMAT, SnowflakeDataTypeConstants.SNOWFLAKE_DATETIME);
		
		case SnowflakeDataTypeConstants.SNOWFLAKE_NUMBERTYPE:
		case SnowflakeDataTypeConstants.SNOWFLAKE_FLOAT:
		case SnowflakeDataTypeConstants.SNOWFLAKE_FLOATTYPE:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_NUMBER);
		
		default:
			return new JSONObject().appendField(SNOWFLAKE_TYPE,SnowflakeDataTypeConstants.SNOWFLAKE_CHARACTER);
		}
		
	}

	/**
	 * Create a Response Profile for Create Operation
	 * @return ObjectDefinition
	 */
	private static ObjectDefinition createResponseProfileForCreate() {
		ObjectDefinition resDef = new ObjectDefinition();
		resDef.setInputType(ContentType.JSON);
		resDef.setOutputType(ContentType.JSON);
		resDef.setElementName("");
		JSONObject json = new JSONObject();
		json.appendField(SNOWFLAKE_SCHEMA_JSON, SCHEMA_NAME);
		json.appendField(SNOWFLAKE_TYPE, SNOWFLAKE_ARRAY);
		json.appendField(SNOWFLAKE_TITLE, SNOWFLAKE_ARRAY_NAME);
		JSONObject jsonArrayObj = new JSONObject();
		jsonArrayObj.appendField(SNOWFLAKE_TYPE, SNOWFLAKE_OBJECT);
		jsonArrayObj.appendField(SNOWFLAKE_TITLE, SNOWFLAKE_OBJECT_NAME);
		JSONObject definition = new JSONObject();
		definition.appendField(SNOWFLAKE_UPDATE_COUNT,new JSONObject().appendField(SNOWFLAKE_TYPE,
				SnowflakeDataTypeConstants.SNOWFLAKE_NUMBER));
		jsonArrayObj.appendField(SNOWFLAKE_PROPERTIES, definition);
		json.appendField(SNOWFLAKE_ITEMS, jsonArrayObj);
		resDef.setJsonSchema(json.toString());
		return resDef;
	}

	/**
	 * Overrides connection settings in the given {@link ObjectDefinitions} if the override flag is set.
	 * Uses properties from the provided {@link PropertyMap} to update the {@link ObjectDefinitions}.
	 *
	 * @param operationProperties the {@link PropertyMap} containing the properties to use for overriding the
	 *                            connection settings.
	 *                            This includes properties like DATABASE and SCHEMA. The override will only occur if
	 *                            the override flag is enabled.
	 * @param defs                the {@link ObjectDefinitions} object to be updated with the new connection settings.
	 *                            This object will be modified to include operation fields based on the properties
	 *                            from the {@code operationProperties}.
	 */
	public void displayConnectionFields(PropertyMap operationProperties, ObjectDefinitions defs) {
		if (ConnectionOverrideUtil.isConnectionSettingsOverride(operationProperties)) {
			defs.withOperationFields(ImportableField.getOverridableFields(
					operationProperties.getProperty(SnowflakeOverrideConstants.DATABASE),
					operationProperties.getProperty(SnowflakeOverrideConstants.SCHEMA)));
		}
	}

	/**
	 * Overrides the schema value if connection settings override is enabled.
	 * Returns the overridden schema name or the original value if no override is applied.
	 *
	 * @param inputSchema the original schema name to be potentially overridden.
	 *                    If the connection settings override is enabled, this value will be replaced with the
	 *                    overridden schema name.
	 *                    Normalized the schema name for both overridden or without overridden.
	 * @return the overridden schema name if connection settings override is enabled, otherwise the original {@code
	 * inputSchema}.
	 */
	private String overrideSchemaValue(String inputSchema) {
		if (ConnectionOverrideUtil.isConnectionSettingsOverride(getContext().getOperationProperties())) {
			inputSchema = ConnectionOverrideUtil.getSchemaName(getContext().getOperationProperties());
		}
		return ConnectionOverrideUtil.normalizeString(inputSchema);
	}

	/**
	 * Resets the connection by overriding the database and schema if pooling is enabled.
	 * Closes the connection after resetting.
	 */
	private void closeAndResetConnectionIfRequired(SnowflakeConnection snowflakeConnection,Connection connection){
		Deque<RuntimeException> exceptionStack = new LinkedList<>();
		if (null != connection && ConnectionOverrideUtil.isConnectionSettingsOverride(
				getContext().getOperationProperties()) && getContext().getConnectionProperties().getBooleanProperty(
				SnowflakeOverrideConstants.PROP_ENABLE_POOLING, false)) {
			ConnectionOverrideUtil.resetConnection(snowflakeConnection, connection, exceptionStack);
		}

		// Ensure the connection is closed in the end, regardless of any exception
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (SQLException e) {
			exceptionStack.push(new ConnectorException("Unable to close Snowflake connection ", e));
		}

		if (!exceptionStack.isEmpty()) {
			throw exceptionStack.pop();
		}
    }
}