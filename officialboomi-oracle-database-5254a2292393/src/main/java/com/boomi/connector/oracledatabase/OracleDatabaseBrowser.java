// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.oracledatabase;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.JSONObject;
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
import com.boomi.connector.oracledatabase.util.ImportableUtil;
import com.boomi.connector.oracledatabase.util.OracleDatabaseConstants;
import com.boomi.connector.oracledatabase.util.ProcedureMetaDataUtil;
import com.boomi.connector.oracledatabase.util.QueryBuilderUtil;
import com.boomi.connector.oracledatabase.util.SchemaBuilderUtil;
import com.boomi.connector.util.BaseBrowser;

/**
 * The Class DatabaseConnectorBrowser.
 *
 * @author swastik.vn
 */
public class OracleDatabaseBrowser extends BaseBrowser implements ConnectionTester {

	/**
	 * Instantiates a new database connector browser.
	 *
	 * @param conn the conn
	 */
	public OracleDatabaseBrowser(OracleDatabaseConnection conn) {
		super(conn);
	}

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(OracleDatabaseBrowser.class.getName());

	/** The Constant DATABASE_BATCHING. */
	private static final String DATABASE_BATCHING = "documentBatching";
	
	/**
	 * Gets the object definitions.
	 *
	 * @param objectTypeId the object type id
	 * @param roles        the roles
	 * @return the object definitions
	 */
	@SuppressWarnings({"java:S3776"})
	@Override
	public ObjectDefinitions getObjectDefinitions(String objectTypeId, Collection<ObjectDefinitionRole> roles) {

		String customOpsType = getContext().getCustomOperationType();
		OperationType opsType = getContext().getOperationType();
		PropertyMap map = getContext().getOperationProperties();
		String getType = (String) map.get(OracleDatabaseConstants.GET_TYPE);
		String updateType = (String) map.get(OracleDatabaseConstants.TYPE);
		String deleteType = (String) map.get(OracleDatabaseConstants.DELETE_TYPE);
		String insertType = (String) map.get(OracleDatabaseConstants.INSERTION_TYPE);
		String upsertType = (String) map.get("upsertType");
		boolean enableQuery = map.getBooleanProperty("enableQuery", false);
		boolean inCLuase = map.getBooleanProperty("INClause", false);
		boolean refCursor = map.getBooleanProperty("refCursor", false);
		ObjectDefinitions objdefs = new ObjectDefinitions();
		boolean isBatching = false;
		OracleDatabaseConnection conn = getConnection();

		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			String schemaName = getContext().getOperationProperties().getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			String packageName = objectTypeId;
			for (ObjectDefinitionRole role : roles) {
				ObjectDefinition objdef = new ObjectDefinition();
				String jsonSchema = null;
				switch (role) {
				case OUTPUT:
					if (OracleDatabaseConstants.STOREDPROCEDUREWRITE.equals(customOpsType)) {
						if (!refCursor) {
							if (new ProcedureMetaDataUtil(con, objectTypeId, packageName).getDataType().containsValue(2012)) {
								throw new ConnectorException(
										"Please select Refcursor checkbox in import wizard if the Procedure is returning Refcursor");
							}
							List<String> outParams = new ProcedureMetaDataUtil(con, objectTypeId, packageName).getOutParams();
							jsonSchema = SchemaBuilderUtil.getProcedureSchema(con, objectTypeId, outParams);
						}
					} else if (OracleDatabaseConstants.GET.equals(customOpsType)) {
						if (getContext().getOperationProperties() != null
								&& !getContext().getOperationProperties().isEmpty()) {
							JSONObject jsonCookie = new JSONObject();
							isBatching = getContext().getOperationProperties().getBooleanProperty(DATABASE_BATCHING,
									false);
							jsonCookie.put(DATABASE_BATCHING, isBatching);
							objdef.withCookie(jsonCookie.toString());
						}
						jsonSchema = SchemaBuilderUtil.getJsonSchema(con, objectTypeId, false, true, isBatching);

					} else {
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema("");

					}
					if (jsonSchema == null) {
						objdefs = this.getUnstructuredSchema(objdef, objdefs);
					} else {
						objdefs = this.getJsonStructure(jsonSchema, objdef, objdefs, false);
					}
					break;

				case INPUT:
					if (OracleDatabaseConstants.STOREDPROCEDUREWRITE.equals(customOpsType)) {
						List<String> inParams = new ProcedureMetaDataUtil(con, objectTypeId,packageName ).getInParams();
						jsonSchema = SchemaBuilderUtil.getProcedureSchema(con, objectTypeId, inParams);
					} else if (OracleDatabaseConstants.DYNAMIC_UPDATE.equals(updateType)) {
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema(updateType);
					} else if (OracleDatabaseConstants.DYNAMIC_DELETE.equals(deleteType)) {
						jsonSchema = SchemaBuilderUtil.getQueryJsonSchema(deleteType);
					} else if (OracleDatabaseConstants.DYNAMIC_INSERT.equals(insertType)
							|| OracleDatabaseConstants.DYNAMIC_GET.equals(getType)&& !inCLuase) {
						jsonSchema = SchemaBuilderUtil.getJsonSchema(con, objectTypeId, false, false, false);
					} else if (OracleDatabaseConstants.STANDARD_INSERT.equals(insertType)
							|| OracleDatabaseConstants.STANDARD_UPDATE.equals(updateType)
							|| OracleDatabaseConstants.STANDARD_DELETE.equals(deleteType)) {
						jsonSchema = SchemaBuilderUtil.getStandardJsonSchema(con, objectTypeId, enableQuery);
					} else if (OperationType.UPSERT.equals(opsType)) {
						jsonSchema = SchemaBuilderUtil.getJsonSchema(con, objectTypeId, enableQuery, false, false);
					} else if ((OracleDatabaseConstants.STANDARD_GET.equals(getType)|| OracleDatabaseConstants.DYNAMIC_GET.equals(getType)) && inCLuase) {
						jsonSchema = SchemaBuilderUtil.getJsonArraySchema(con, objectTypeId, enableQuery);
					} else {
						jsonSchema = SchemaBuilderUtil.getJsonSchema(con, objectTypeId, enableQuery, false, false);
					}
					if (jsonSchema != null) {
						objdefs = this.getJsonStructure(jsonSchema, objdef, objdefs, inCLuase);
					} else {
						objdefs = this.getUnstructuredSchema(objdef, objdefs);
					}
					break;
				default:
					break;
				}
			}
			if (!OracleDatabaseConstants.STOREDPROCEDUREWRITE.equals(customOpsType) && !objectTypeId.contains(",")) {
				objdefs.getOperationFields()
						.add(createSimpleField(con, opsType, customOpsType, objectTypeId, upsertType));
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}

		return objdefs;
	}

	/**
	 * Creates the simple field.
	 *
	 * @param con           the con
	 * @param opsType       the ops type
	 * @param customOpsType the custom ops type
	 * @param objectTypeId  the object type id
	 * @param upsertType
	 * @return the browse field
	 */
	public BrowseField createSimpleField(Connection con, OperationType opsType, String customOpsType,
			String objectTypeId, String upsertType) {
		String query;
		try {
			ImportableUtil importableUtil = new ImportableUtil(con, objectTypeId);
			query = importableUtil.buildImportableFields(opsType, customOpsType, upsertType);
		} catch (SQLException e) {
			throw new ConnectorException("Unable to build importable field for table " + objectTypeId);
		}
		BrowseField simpleField = new BrowseField();
		simpleField.setId(OracleDatabaseConstants.QUERY);
		simpleField.setLabel("SQL Query");
		simpleField.setType(DataType.STRING);
		simpleField.setDisplayType(DisplayType.TEXTAREA);
		simpleField.setDefaultValue(query);
		return simpleField;
	}

	/**
	 * Method that will take {@link ObjectDefinition} and build the unstructured
	 * Schema for request and response profile.
	 *
	 * @param objdef  the objdef
	 * @param objdefs the objdefs
	 * @return objdefs
	 */
	private ObjectDefinitions getUnstructuredSchema(ObjectDefinition objdef, ObjectDefinitions objdefs) {
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
	 * @param inCLuase   the in C luase
	 * @return objdefs
	 */
	private ObjectDefinitions getJsonStructure(String jsonSchema, ObjectDefinition objdef, ObjectDefinitions objdefs,
			boolean inCLuase) {
		objdef.setElementName("");
		objdef.setJsonSchema(jsonSchema);
		objdef.setOutputType(ContentType.JSON);
		objdef.setInputType(ContentType.JSON);
		if (inCLuase) {
			objdef.setCookie("inClause");
		}
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

		ObjectTypes objtypes = new ObjectTypes();
		List<ObjectType> objTypeList = new ArrayList<>();
		OracleDatabaseConnection conn = getConnection();
		ResultSet resultSet = null;
		try (Connection con = conn.getOracleConnection()) {
			if (con == null) {
				throw new ConnectorException("connection failed , please check connection details");
			}
			DatabaseMetaData md = con.getMetaData();
			PropertyMap operationProperties = getContext().getOperationProperties();
			String schemaName = operationProperties.getProperty(OracleDatabaseConstants.SCHEMA_NAME);
			QueryBuilderUtil.setSchemaName(con, conn, schemaName);
			String opsType = getContext().getCustomOperationType();
			if (opsType != null && opsType.equals(OracleDatabaseConstants.STOREDPROCEDUREWRITE)) {
				String procedureNamePattern = operationProperties.getProperty(
						OracleDatabaseConstants.PROCEDURE_NAME_PATTERN);
				String sqlWildcardPattern = Optional.ofNullable(
						QueryBuilderUtil.replaceWithSqlWildCards(procedureNamePattern)).orElse("%");
				resultSet = md.getProcedures(null, con.getSchema(), sqlWildcardPattern);
				while (resultSet.next()) {
					getObjectTypesValue(objtypes, resultSet);
				}
			} else {
				checkTablName(objtypes, objTypeList, con, md, opsType);
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unable to get the table names from the database {0}", e.getMessage());
		} finally {
			if (resultSet != null) {
				try {
					resultSet.close();
				} catch (SQLException e) {
					logger.log(Level.SEVERE, "Result set not closed properly!!");
				}
			}
		}
		return objtypes;
	}

	/**
	 * 
	 * @param objtypes
	 * @param resultSet
	 * @throws SQLException
	 */
	private void getObjectTypesValue(ObjectTypes objtypes, ResultSet resultSet) throws SQLException {
		String cat = null;
		String fun = null;
		String procedureName = resultSet.getString(OracleDatabaseConstants.PROCEDURE_NAME);
		
		if (resultSet.getString(OracleDatabaseConstants.PROCEDURE_CAT) != null) {
			cat = resultSet.getString(OracleDatabaseConstants.PROCEDURE_CAT);
		}
		if (resultSet.getString(OracleDatabaseConstants.PROCEDURE_TYPE) != null) {
			fun = resultSet.getString(OracleDatabaseConstants.PROCEDURE_TYPE);
		}
		ObjectType objtype = new ObjectType();
		if (cat != null && fun!=null && fun.equals("1") ) {
			objtype.setId(cat + "." + procedureName);
			objtype.setLabel(cat + "." + procedureName + " (" + "SP"+ ") ");
		}
		else if (cat != null && fun!=null && fun.equals("2") ) {
			objtype.setId(cat + "." + procedureName);
			objtype.setLabel(cat + "." + procedureName + " (" + "Function"+ ") ");
		}
		else if(fun!=null && fun.equals("1")) {
			objtype.setId(procedureName);
			objtype.setLabel(procedureName + " (" + "SP" + ")");
		}
		else if(fun!=null && fun.equals("2")) {
			objtype.setId(procedureName);
			objtype.setLabel(procedureName + " (" + "Function" + ") ");
		}
		objtypes.getTypes().add(objtype);
	}

	/**
	 * 
	 * @param objtypes
	 * @param objTypeList
	 * @param con
	 * @param md
	 * @param opsType
	 * @throws SQLException
	 */
	private void checkTablName(ObjectTypes objtypes, List<ObjectType> objTypeList, Connection con, DatabaseMetaData md,
			String opsType) throws SQLException {
		String tableNames = getContext().getOperationProperties().getProperty("tableNames", null);
		if (tableNames != null) {
			ObjectType objType = this.validate(tableNames, con);
			objTypeList.add(objType);
		} else {
			if(opsType != null && opsType.equals(OracleDatabaseConstants.GET)) {
				try(ResultSet resultSet1 = md.getTables(con.getCatalog(), con.getSchema(), null,
						new String[] { OracleDatabaseConstants.TABLE , OracleDatabaseConstants.VIEWS });){
					while (resultSet1.next()) {
						objTypeList.add(this.setObjectType(resultSet1));
					}
				}
			}else {
				try(ResultSet resultSet2 = md.getTables(con.getCatalog(), con.getSchema(), null,
						new String[] { OracleDatabaseConstants.TABLE });){
					while (resultSet2.next()) {
						objTypeList.add(this.setObjectType(resultSet2));
					}
				}
			}
			
		}
		objtypes.getTypes().addAll(objTypeList);
	}

	/**
	 * This method is to validate whether the table names provided by user exists in
	 * the database by Querying the particular table and fetching the 1st row if
	 * record exists.
	 *
	 * @param tableNames the table names
	 * @param con        the con
	 * @return the object type
	 * @throws SQLException the SQL exception
	 */
	private ObjectType validate(String tableNames, Connection con) throws SQLException {
		String[] tableName = tableNames.split("[,]", 0);
		ObjectType objectType = new ObjectType();
		for (String table : tableName) {
			try (PreparedStatement pstmnt = con.prepareStatement("SELECT count(*) FROM " + table.trim())) {
				pstmnt.setMaxRows(1);
				try (ResultSet ignored = pstmnt.executeQuery()) {
					logger.log(Level.FINE,"{0} exists!!", table);
				}
			} catch (SQLException e) {
				throw new ConnectorException(e.getMessage());
			}
		}
		if (con.getMetaData().getDatabaseProductName().equals("Oracle")) {
			objectType.setId(tableNames.toUpperCase());
		} else {
			objectType.setId(tableNames);
		}

		return objectType;

	}

	/**
	 * Sets the object Type for Single table
	 * @param resultSet
	 * @throws SQLException
	 * @return objtype
	 */
	private ObjectType setObjectType(ResultSet resultSet) throws SQLException {
		ObjectType objtype = new ObjectType();
		objtype.setId(resultSet.getString(OracleDatabaseConstants.TABLE_NAME));
		objtype.setLabel(resultSet.getString(OracleDatabaseConstants.TABLE_NAME).concat(" (").concat(resultSet.getString(4)).concat(") "));
		return objtype;
	}
	
	/**
	 * Gets Connection Object.
	 *
	 * @return the connection
	 */
	@Override
	public OracleDatabaseConnection getConnection() {
		return (OracleDatabaseConnection) super.getConnection();
	}

	/**
	 * Method to test the database Connection by taking connection parameters.
	 */
	@Override
	public void testConnection() {
		getConnection().test();
	}
}