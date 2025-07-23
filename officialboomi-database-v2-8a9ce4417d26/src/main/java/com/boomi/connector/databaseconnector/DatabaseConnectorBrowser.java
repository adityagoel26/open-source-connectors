// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector;

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
import com.boomi.connector.api.ui.BrowseField;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.api.ui.DisplayType;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.util.ImportableUtil;
import com.boomi.connector.databaseconnector.util.ProcedureMetaDataUtil;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.databaseconnector.util.SchemaBuilderUtil;
import com.boomi.connector.util.BaseBrowser;
import com.boomi.util.StringUtil;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * The Class DatabaseConnectorBrowser.
 *
 * @author swastik.vn
 */
public class DatabaseConnectorBrowser extends BaseBrowser implements ConnectionTester {

	/**
	 * Instantiates a new database connector browser.
	 *
	 * @param databaseConnectorConnection the databaseConnectorConnection
	 */
	public DatabaseConnectorBrowser(DatabaseConnectorConnection databaseConnectorConnection) {
		super(databaseConnectorConnection);
	}

	/** The Constant LOG. */
	private static final Logger LOG = Logger.getLogger(DatabaseConnectorBrowser.class.getName());
	/** The Constant DATABASE_BATCHING. */
	private static final String DATABASE_BATCHING = "documentBatching";

	/** Constant COLUMN_INDEX_FOUR is to Fetch Object Type from metadata */
	private static final int COLUMN_INDEX_FOUR = 4;

	/**
	 * Gets the object definitions.
	 *
	 * @param objectTypeId the object type id
	 * @param roles        the roles
	 * @return the object definitions
	 */
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {

		String customOpsType = getContext().getCustomOperationType();
		OperationType opsType = getContext().getOperationType();
		String getType = (String) getContext().getOperationProperties().get(DatabaseConnectorConstants.GET_TYPE);
		String updateType = (String) getContext().getOperationProperties().get(DatabaseConnectorConstants.TYPE);
		String deleteType = (String) getContext().getOperationProperties().get(DatabaseConnectorConstants.DELETE_TYPE);
		String insertType = (String) getContext().getOperationProperties()
				.get(DatabaseConnectorConstants.INSERTION_TYPE);
		boolean enableQuery = getContext().getOperationProperties().getBooleanProperty("enableQuery", false);
		ObjectDefinitions objdefs = new ObjectDefinitions();
		boolean isBatching = false;
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		String schemaName = (String) getContext().getOperationProperties().get(DatabaseConnectorConstants.SCHEMA_NAME);

		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if (sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, databaseConnectorConnection.getSchemaName());
			String schema = QueryBuilderUtil.getSchemaFromConnection(
					sqlConnection.getMetaData().getDatabaseProductName(), sqlConnection, schemaName,
					databaseConnectorConnection.getSchemaName());
			for (ObjectDefinitionRole role : roles) {
				ObjectDefinition objdef = new ObjectDefinition();
				String jsonSchema = null;
				switch (role) {
				case OUTPUT:
					if (OperationTypeConstants.STOREDPROCEDUREWRITE.equals(customOpsType)) {
						String procedure = SchemaBuilderUtil.getProcedureName(objectTypeId);
						String packageName = SchemaBuilderUtil.getProcedurePackageName(objectTypeId);
						List<String> outParams = ProcedureMetaDataUtil.getOutputParams(sqlConnection, procedure, packageName, schema);
						jsonSchema = SchemaBuilderUtil.getProcedureSchema(sqlConnection, objectTypeId, outParams, schema);

					} else if (DatabaseConnectorConstants.GET.equals(customOpsType)) {
						if (getContext().getOperationProperties() != null
								&& !getContext().getOperationProperties().isEmpty()) {
							JSONObject jsonCookie = new JSONObject();
							isBatching = getContext().getOperationProperties().getBooleanProperty(DATABASE_BATCHING,
									false);
							jsonCookie.put(DATABASE_BATCHING, isBatching);
							objdef.withCookie(jsonCookie.toString());
						}
						jsonSchema = SchemaBuilderUtil.getJsonSchema(sqlConnection, objectTypeId, false, true, isBatching, schema);

					} else {
						//Adding cookie to objDef
						//GET and STOREDPROCEDURE actions as well could be benefit from the approach (CONC-10175)
						JSONObject jsonCookie = new JSONObject();
						jsonCookie.put(DatabaseConnectorConstants.COLUMN_NAMES_KEY,
								getColumnNamesAsJsonArray(sqlConnection, objectTypeId, schema));
						objdef.withCookie(jsonCookie.toString());
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema("");
					}
					if (jsonSchema == null) {
						objdefs = getUnstructuredSchema(objdef, objdefs);
					} else {
						objdefs = getJsonStructure(jsonSchema, objdef, objdefs);
					}

					break;

				case INPUT:
					if (OperationTypeConstants.STOREDPROCEDUREWRITE.equals(customOpsType)) {
						String procedure = SchemaBuilderUtil.getProcedureName(objectTypeId);
						String packageName = SchemaBuilderUtil.getProcedurePackageName(objectTypeId);
						List<String> inParams = ProcedureMetaDataUtil.getInputParams(sqlConnection, procedure, packageName, schema);
						jsonSchema = SchemaBuilderUtil.getProcedureSchema(sqlConnection, objectTypeId, inParams, schema);
					} else if (OperationTypeConstants.DYNAMIC_UPDATE.equals(updateType)) {
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema(updateType);
					} else if (OperationTypeConstants.DYNAMIC_DELETE.equals(deleteType)) {
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema(deleteType);
					} else if (OperationTypeConstants.DYNAMIC_INSERT.equals(insertType)
							|| OperationTypeConstants.DYNAMIC_GET.equals(getType)
							|| OperationType.UPSERT.equals(opsType)) {
						jsonSchema = SchemaBuilderUtil.getJsonSchema(sqlConnection, objectTypeId, false, false, false, schema);
					}

					else {
						jsonSchema = SchemaBuilderUtil.getJsonSchema(sqlConnection, objectTypeId, enableQuery, false, false, schema);
					}
					if (jsonSchema != null) {
						objdefs = getJsonStructure(jsonSchema, objdef, objdefs);
					} else {
						objdefs = getUnstructuredSchema(objdef, objdefs);
					}
					if (!StringUtil.isEmpty(schemaName)) {
						objdef.setCookie(schemaName);
					}
					break;
				default:
					break;
				}
				LOG.info(jsonSchema);
			}
			if (!OperationTypeConstants.STOREDPROCEDUREWRITE.equals(customOpsType)
					&& !OperationType.UPSERT.equals(opsType) && !objectTypeId.contains(",")) {
				objdefs.getOperationFields().add(
						createSqlQuerySimpleField(sqlConnection, opsType, customOpsType, objectTypeId, schema));
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage(), e);
		}
		return objdefs;
	}

	/**
	 * Retrieves column details as a JSON object for the specified table/objectType
	 *
	 * @param sqlConnection Database connection
	 * @param objectTypeId  Table or object name
	 * @param schema        Schema name
	 * @return JSONObject containing column metadata
	 * @throws SQLException if database access error occurs
	 */
	private static JSONArray getColumnNamesAsJsonArray(Connection sqlConnection, String objectTypeId, String schema)
			throws SQLException {

		// Initialize JSON object to store column metadata
		JSONArray columnNameArray = new JSONArray();

		// Retrieve all column names for the specified table
		Set<String> columnResultSet = QueryBuilderUtil.getTableColumnsAsSet(sqlConnection.getCatalog(), schema,
				sqlConnection, objectTypeId);

		for (String columnName : columnResultSet) {
			columnNameArray.put(columnName);
		}

		return columnNameArray;
	}

        /**
	 * Creates the simple field to display the generated SQL Query.
	 *
	 * @param sqlConnection the sqlConnection
	 * @param opsType       the ops type
	 * @param customOpsType the custom ops type
	 * @param objectTypeId  the object type id
	 * @param schemaName the schema name
	 * @return the browse field
	 * @throws IOException
	 */
	public BrowseField createSqlQuerySimpleField(Connection sqlConnection, OperationType opsType, String customOpsType,
			String objectTypeId, String schemaName) {
		String query;
		try {
			ImportableUtil importableUtil = new ImportableUtil(sqlConnection, objectTypeId);
			query = importableUtil.buildImportableFields(opsType, customOpsType, schemaName);
		} catch (SQLException e) {
			throw new ConnectorException("Unable to build importable field for table " + objectTypeId, e);
		}
		BrowseField simpleField = new BrowseField();
		simpleField.setId(DatabaseConnectorConstants.QUERY);
		simpleField.setLabel("SQL Query");
		simpleField.setType(DataType.STRING);
		simpleField.setDisplayType(DisplayType.TEXTAREA);
		simpleField.setDefaultValue(query);
		simpleField.setHelpText(getHelpTextSqlQueryByOperationType(opsType, customOpsType));
		return simpleField;
	}

	/**
	 * Returns help text for SQL queries based on the operation type.
	 *
	 * @param opsType       The operation type enum value
	 * @param customOpsType The custom operation type string
	 * @return Formatted help text string specific to the operation type
	 */
	private static String getHelpTextSqlQueryByOperationType(OperationType opsType, String customOpsType) {
        String helpText =
                "Type or paste a SQL prepared statement that is valid for the %s statement. For more than one "
                        + "statement, separate by semicolon and append a connection property allowMultiQueries=true to"
                        + " the database url.";
        String operation;
        String key = (opsType != null ? opsType.toString() : "") + ":" + (customOpsType != null ? customOpsType : "");
        switch (key) {
            case "CREATE:CREATE":
                operation = "Insert";
                break;
            case "UPDATE:":
                operation = "Update";
                break;
            case "EXECUTE:GET":
                operation = "Get";
                break;
            case "EXECUTE:DELETE":
                operation = "Delete";
                break;
            default:
                return helpText;
        }
        return String.format(helpText, operation);
    }

	/**
	 * Method that will take {@link ObjectDefinition} and build the unstructured
	 * Schema for request and response profile.
	 *
	 * @param objdef  the objdef
	 * @param objdefs the objdefs
	 * @return objdefs
	 */
	private static ObjectDefinitions getUnstructuredSchema(ObjectDefinition objdef, ObjectDefinitions objdefs) {
		objdef.setElementName("");
		objdef.setOutputType(ContentType.BINARY);
		objdef.setInputType(ContentType.NONE);

		objdefs.getDefinitions().add(objdef);
		return objdefs;
	}

	/**
	 * This method will take {@link ObjectDefinition} and jsonSchema and it will
	 * build the structured Schema for request and response profile.
	 *
	 * @param jsonSchema the json schema
	 * @param objdef     the objdef
	 * @param objdefs    the objdefs
	 * @return objdefs
	 */
	private static ObjectDefinitions getJsonStructure(String jsonSchema, ObjectDefinition objdef,
			ObjectDefinitions objdefs) {
		objdef.setElementName("");
		objdef.setJsonSchema(jsonSchema);
		objdef.setOutputType(ContentType.JSON);
		objdef.setInputType(ContentType.JSON);
		objdefs.getDefinitions().add(objdef);
		return objdefs;
	}

	/**
	 * This method will add the table names or the procedure names to the object
	 * type list based on the operation selected.
	 *
	 * @return the object types
	 */
	@Override
	public ObjectTypes getObjectTypes() {
		PropertyMap operationProperties = getContext().getOperationProperties();
		String      schemaName          = operationProperties.getProperty(DatabaseConnectorConstants.SCHEMA_NAME);
		ObjectTypes objtypes = new ObjectTypes();
		List<ObjectType> objTypeList = new ArrayList<>();
		DatabaseConnectorConnection databaseConnectorConnection = getConnection();
		try (Connection sqlConnection = databaseConnectorConnection.getDatabaseConnection()) {
			if (sqlConnection == null) {
				throw new ConnectorException(
						DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
			}
			DatabaseMetaData md = sqlConnection.getMetaData();
			if ((!DatabaseConnectorConstants.ORACLE.equals(md.getDatabaseProductName()) && StringUtil.isEmpty(
					schemaName) && databaseConnectorConnection.getSchemaName().isEmpty() && sqlConnection.getCatalog()
					.isEmpty()) || (DatabaseConnectorConstants.POSTGRESQL.equals(md.getDatabaseProductName())
					&& databaseConnectorConnection.getSchemaName().isEmpty() && StringUtil.isEmpty(schemaName)
					&& sqlConnection.getSchema().isEmpty())) {
				throw new ConnectorException(
						"Please specify schema name in one of the place - Operation UI / Connection URL / Schema name Field");
			}
			QueryBuilderUtil.setSchemaNameInConnection(sqlConnection, schemaName, databaseConnectorConnection.getSchemaName());
			String schema = QueryBuilderUtil.getSchemaFromConnection(md.getDatabaseProductName(), sqlConnection, schemaName,
                    databaseConnectorConnection.getSchemaName());
			String opsType = getContext().getCustomOperationType();
			if (OperationTypeConstants.STOREDPROCEDUREWRITE.equals(opsType)) {
				String procedureNamePattern = operationProperties.getProperty(DatabaseConnectorConstants.PROCEDURE_NAME_PATTERN);
				String sqlWildcardPattern = Optional.ofNullable(
						QueryBuilderUtil.replaceWithSqlWildCards(procedureNamePattern)).orElse("%");
				try(ResultSet resultSet = md.getProcedures(sqlConnection.getCatalog(), schema, sqlWildcardPattern)){
					while (resultSet.next()) {
						String procedurePackageName = null;
						String procedureName = resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME);
						if(md.getDatabaseProductName().equals(DatabaseConnectorConstants.ORACLE) &&
								resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME) != null) {
							procedurePackageName = resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME);
						}
						if (md.getDatabaseProductName().equals(DatabaseConnectorConstants.MSSQLSERVER)) {
							ObjectType objtype = new ObjectType();
							objtype.setId(procedureName.substring(0, procedureName.length() - 2));
							objTypeList.add(objtype);
						} else {
							ObjectType objtype = new ObjectType();
							if(md.getDatabaseProductName().equals(DatabaseConnectorConstants.ORACLE) && procedurePackageName != null) {
								objtype.setId(procedurePackageName+"."+procedureName);
							}else {
								objtype.setId(procedureName);
							}
							objTypeList.add(objtype);
						}
					}
				}
			} else {
				String tableNames = getContext().getOperationProperties().getProperty("tableNames", null);
				if (tableNames != null) {
					ObjectType objType = validate(tableNames, sqlConnection, schema);
					objTypeList.add(objType);
				} else {
					if(opsType != null && DatabaseConnectorConstants.GET.equals(opsType)) {
						try(ResultSet resultSet = md.getTables(sqlConnection.getCatalog(), schema, null,
								new String[] { DatabaseConnectorConstants.TABLE , DatabaseConnectorConstants.VIEWS });){
							while (resultSet.next()) {
								objTypeList.add(setObjectType(resultSet));
							}
						}
					}else {
						try(ResultSet resultSet = md.getTables(sqlConnection.getCatalog(), schema, null,
								new String[] { DatabaseConnectorConstants.TABLE });){
							while (resultSet.next()) {
								objTypeList.add(setObjectType(resultSet));
							}
						}
					}

				}
			}
		} catch (Exception e) {
			throw new ConnectorException(e.getMessage(), e);
		}
		objtypes.getTypes().addAll(objTypeList);
		return objtypes;
	}

	/**
	 * This method is to validate whether the table names provided by user exists in
	 * the database by Querying the particular table and fetching the 1st row if
	 * record exists.
	 *
	 * @param tableNames the table names
	 * @param sqlConnection the sqlConnection
	 * @param schemaName the schema name
	 * @return the object type
	 * @throws SQLException
	 */
	private static ObjectType validate(String tableNames, Connection sqlConnection, String schemaName)
			throws SQLException {
		String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
		String[] tableName = tableNames.split("[,]", 0);
		ObjectType objectType = new ObjectType();
		StringBuilder objectLabel = new StringBuilder();
		for (String table : tableName) {
			String finalTableName = QueryBuilderUtil.checkTableName(table, databaseName, schemaName);

			try (PreparedStatement pstmnt = sqlConnection.prepareStatement(
					"SELECT count(*) FROM " + finalTableName.trim())) {
				pstmnt.setMaxRows(1);
				try (ResultSet rs = pstmnt.executeQuery()) {
					DatabaseMetaData md = sqlConnection.getMetaData();

					if (databaseName.equals(DatabaseConnectorConstants.MYSQL)) {
						table = "`" + table + "`";
					}
					try (ResultSet resultSet = md.getTables(sqlConnection.getCatalog(), schemaName, table,
							new String[] { DatabaseConnectorConstants.TABLE , DatabaseConnectorConstants.VIEWS });){

						while (resultSet.next()) {
							if(objectLabel.length() == 0) {
								objectLabel.append(resultSet.getString(DatabaseConnectorConstants.TABLE_NAME))
								.append(" (").append(resultSet.getString(COLUMN_INDEX_FOUR)).append(")");
							}
							else {
								objectLabel.append(", ").append(
										resultSet.getString(DatabaseConnectorConstants.TABLE_NAME)).append(" (").append(
										resultSet.getString(COLUMN_INDEX_FOUR)).append(")");
							}
						}
					}
				}
			} catch (SQLException e) {
				throw new ConnectorException(e.getMessage(), e);
			}
		}
		if (DatabaseConnectorConstants.ORACLE.equals(sqlConnection.getMetaData().getDatabaseProductName())) {
			objectType.setId(tableNames.toUpperCase());
			if(objectLabel.length()>0) {
				objectType.setLabel(objectLabel.toString().toUpperCase());
			}
		} else {
			objectType.setId(tableNames);
			if(objectLabel.length()>0) {
				objectType.setLabel(objectLabel.toString());
			}
		}

		return objectType;

	}

	/**
	 * Sets the object Type for Single table
	 * @param resultSet
	 * @throws SQLException
	 */
	private static ObjectType setObjectType(ResultSet resultSet) throws SQLException {
		ObjectType objtype = new ObjectType();
		objtype.setId(resultSet.getString(DatabaseConnectorConstants.TABLE_NAME));
		objtype.setLabel(resultSet.getString(DatabaseConnectorConstants.TABLE_NAME) + " (" + resultSet.getString(
				COLUMN_INDEX_FOUR) + ") ");
		return objtype;
	}

	/**
	 * Gets Connection Object.
	 *
	 * @return the connection
	 */
	@Override
	public DatabaseConnectorConnection getConnection() {
		return (DatabaseConnectorConnection) super.getConnection();
	}

	/**
	 * Method to test the database Connection by taking connection parameters.
	 */
	@Override
	public void testConnection() {
		getConnection().test();
	}
}