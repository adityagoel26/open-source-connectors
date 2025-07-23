// Copyright (c) 2020 Boomi, LP.
package com.boomi.connector.oracledatabase.util;

import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.oracledatabase.model.DeletePojo;
import com.boomi.connector.oracledatabase.model.QueryResponse;
import com.boomi.connector.oracledatabase.model.UpdatePojo;
import com.boomi.connector.oracledatabase.params.ExecutionParameters;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

import oracle.jdbc.driver.OracleConnection;
import oracle.sql.ArrayDescriptor;
import oracle.sql.StructDescriptor;

/**
 * This Util Class will Generate the JSON Schema by taking each column name from
 * the table and associated DataTypes of the column. Based on the column
 * datatype the Schema Type will be either String, Boolean or Number.
 * 
 * @author swastik.vn
 *
 */
public class SchemaBuilderUtil {

	/**
	 * Instantiates a new schema builder util.
	 */
	private SchemaBuilderUtil() {

	}
	
	/**
	 * Build Json Schema Based on the Database Column names and column types.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param enableQuery  the enable query
	 * @param output       the output
	 * @param isBatching
	 * @return the json schema
	 */
	public static String getJsonSchema(Connection con, String objectTypeId, boolean enableQuery, boolean output, boolean isBatching) {

		String jsonSchema = null;
		StringBuilder sbSchema = new StringBuilder();
		Map<String, String> dataTypes = new HashMap<>();
		String[] tableNames = getTableNames(objectTypeId);
		try {
			if (objectTypeId.contains(",")) {
				for (String tableName : tableNames) {
					dataTypes.putAll(MetadataUtil.getDataTypesWithTable(con, tableName.trim()));
				}
			} else {
				dataTypes.putAll(new MetadataUtil(con, objectTypeId).getDataType());
			}
			DatabaseMetaData md = con.getMetaData();
			if (isBatching) {
				sbSchema.append("{").append(JSON_DRAFT4_DEFINITION).append(" \"").append(JSONUtil.SCHEMA_TYPE)
						.append("\": \"array\",").append(" \"").append("items\":{").append(" \"").append(JSONUtil.SCHEMA_TYPE)
						.append("\": \"object\",").append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append("\": {");
			} else {
			sbSchema.append("{").append(JSON_DRAFT4_DEFINITION).append(" \"").append(JSONUtil.SCHEMA_TYPE)
					.append(OBJECT_STRING).append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
			}
			for (String tableName : tableNames) {
				ExecutionParameters executionParameters = new ExecutionParameters(con, dataTypes);
				getJsonSchemaValue(executionParameters, objectTypeId, enableQuery, output, sbSchema, md, tableName);
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
		sbSchema.append("}}");
		if (isBatching) {
			sbSchema.append("}");
		}
		JsonNode rootNode = null;
		try {
			rootNode = JSONUtil.getDefaultObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).readTree(sbSchema.toString());
			if (rootNode != null) {
				jsonSchema = JSONUtil.prettyPrintJSON(rootNode);
			}
		} catch (Exception e) {
			throw new ConnectorException(SCHEMA_BUILDER_EXCEPTION, e.getMessage());
		}
		return jsonSchema;
	}

	/**
	 * 
	 * @param con
	 * @param objectTypeId
	 * @param enableQuery
	 * @param output
	 * @param sbSchema
	 * @param dataTypes
	 * @param md
	 * @param tableName
	 * @throws SQLException
	 */
	private static void getJsonSchemaValue(ExecutionParameters executionParameters,  String objectTypeId, boolean enableQuery, boolean output,
			StringBuilder sbSchema, DatabaseMetaData md, String tableName)
			throws SQLException {
		Connection con = executionParameters.getCon();
		Map<String, String> dataTypes = executionParameters.getDataTypes();
		try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), tableName.trim(), null);) {
			while (resultSet.next()) {
				String param = setColumnNames(objectTypeId, tableName, resultSet);
				if (output) {
					sbSchema.append("\"").append(resultSet.getString(COLUMN_NAME)).append(OPEN_PROPERTIES);
				} else {
					sbSchema.append("\"").append(param).append(OPEN_PROPERTIES);
				}
				if (!dataTypes.containsKey(param) && !dataTypes.containsKey(resultSet.getString(COLUMN_NAME))) {
					throw new SQLException("The data type " + resultSet.getString(TYPE_NAME)
							+ " is not supported in the connector!");
				}
				if (STRING.equals(dataTypes.get(param)) || TIME.equals(dataTypes.get(param))
						|| DATE.equals(dataTypes.get(param)) || BLOB.equals(dataTypes.get(param))
						|| NVARCHAR.equals(dataTypes.get(param)) || TIMESTAMP.equals(dataTypes.get(param))) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
				} else if (INTEGER.equals(dataTypes.get(param)) || DOUBLE.equals(dataTypes.get(param))
						|| FLOAT.equals(dataTypes.get(param)) || LONG.equals(dataTypes.get(param))) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
				} else if (BOOLEAN.equals(dataTypes.get(param))) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(BOOLEAN).append(BACKSLASH);
				} else {
					getJsonSchemaArrayValue(con, sbSchema, resultSet);
				}
				sbSchema.append("},");
			}
			if (enableQuery) {
				sbSchema.append(BACKSLASH).append(SQL_QUERY).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
				sbSchema.append("},");
			}
		}
	}

	private static String setColumnNames(String objectTypeId, String tableName, ResultSet resultSet) throws SQLException {
		String param;
		if (objectTypeId.contains(",")) {
			param = tableName.trim() + DOT + resultSet.getString(COLUMN_NAME);
		} else {
			param = resultSet.getString(COLUMN_NAME);
		}
		return param;
	}

	private static void getJsonSchemaArrayValue(Connection con, StringBuilder sbSchema, ResultSet resultSet)
			throws SQLException {
		// array parameter starts
		ArrayDescriptor array = ArrayDescriptor
				.createDescriptor(resultSet.getString(OracleDatabaseConstants.TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
		sbSchema.append(TYPE_OBJECT);
		sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
		boolean type = QueryBuilderUtil.checkArrayDataType(array);
		if(type) {
			iterateOverVarrays(array, sbSchema, con);
		} else {
			iterateOverNestedTables(array, sbSchema, con);
		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
		sbSchema.append('}');
		// array parameter ends
	}

	/**
	 * This method will generate array type of schema for IN Clause
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param enableQuery  the enable query
	 * @return the json array schema
	 */
	public static String getJsonArraySchema(Connection con, String objectTypeId, boolean enableQuery) {
		String jsonSchema = null;
		StringBuilder sbSchema = new StringBuilder();
		Map<String, String> dataTypes = new HashMap<>();
		String[] tableNames = getTableNames(objectTypeId);
		try {
			if (objectTypeId.contains(",")) {
				for (String tableName : tableNames) {
					dataTypes.putAll(MetadataUtil.getDataTypesWithTable(con, tableName.trim()));
				}
			} else {
				dataTypes.putAll(new MetadataUtil(con, objectTypeId).getDataType());
			}
			DatabaseMetaData md = con.getMetaData();
			sbSchema.append("{").append(JSON_DRAFT4_DEFINITION).append(" \"").append(JSONUtil.SCHEMA_TYPE)
					.append(OBJECT_STRING).append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
			for (String tableName : tableNames) {
				getJsonArraySchemaValue(con, objectTypeId, enableQuery, sbSchema, dataTypes, md, tableName);
			}
		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
		sbSchema.append("}}");
		JsonNode rootNode = null;
		try {
			rootNode = JSONUtil.getDefaultObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).readTree(sbSchema.toString());
			if (rootNode != null) {
				jsonSchema = JSONUtil.prettyPrintJSON(rootNode);
			}
		} catch (Exception e) {
			throw new ConnectorException(SCHEMA_BUILDER_EXCEPTION, e.getMessage());
		}
		return jsonSchema;
	}

	/**
	 * 
	 * @param con
	 * @param objectTypeId
	 * @param enableQuery
	 * @param sbSchema
	 * @param dataTypes
	 * @param md
	 * @param tableName
	 * @throws SQLException
	 */
	private static void getJsonArraySchemaValue(Connection con, String objectTypeId, boolean enableQuery,
			StringBuilder sbSchema, Map<String, String> dataTypes, DatabaseMetaData md, String tableName)
			throws SQLException {
		try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), tableName.trim(), null);) {
			while (resultSet.next()) {
				createJsonArraySchema(con, objectTypeId, sbSchema, dataTypes, tableName, resultSet);
			}
			if (enableQuery) {
				sbSchema.append(BACKSLASH).append(SQL_QUERY).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING)
						.append(BACKSLASH);
				sbSchema.append("},");
			}
		}
	}

	private static void createJsonArraySchema(Connection con, String objectTypeId, StringBuilder sbSchema,
			Map<String, String> dataTypes, String tableName, ResultSet resultSet) throws SQLException {
		String param = setColumnNames(objectTypeId, tableName, resultSet);
		sbSchema.append(BACKSLASH + param + OPEN_PROPERTIES);
		if (STRING.equals(dataTypes.get(param)) || TIME.equals(dataTypes.get(param))
				|| DATE.equals(dataTypes.get(param)) || BLOB.equals(dataTypes.get(param))
				|| NVARCHAR.equals(dataTypes.get(param)) || TIMESTAMP.equals(dataTypes.get(param))) {
			sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS).append("\"type\": \"string\"\r\n")
					.append(CLOSE_ARRAY);
		} else if (INTEGER.equals(dataTypes.get(param)) || DOUBLE.equals(dataTypes.get(param))
				|| FLOAT.equals(dataTypes.get(param)) || LONG.equals(dataTypes.get(param))) {
			sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS).append("\"type\": \"integer\"\r\n")
					.append(CLOSE_ARRAY);
		} else if (BOOLEAN.equals(dataTypes.get(param))) {
			sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS).append("\"type\": \"boolean\"\r\n")
					.append(CLOSE_ARRAY);
		} else {
			getJsonSchemaArrayValue(con, sbSchema, resultSet);
		}
		sbSchema.append("},");
	}

	/**
	 * This Method will build the Json Schema for all the Standard Operations.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param enableQuery  the enable query
	 * @return the standard json schema
	 */
	public static String getStandardJsonSchema(Connection con, String objectTypeId, boolean enableQuery) {

		String jsonSchema = null;
		StringBuilder sbSchema = new StringBuilder();
		Map<String, String> dataTypes;
		try {
			dataTypes = new MetadataUtil(con, objectTypeId).getDataType();
			DatabaseMetaData md = con.getMetaData();
			sbSchema.append("{").append(JSON_DRAFT4_DEFINITION).append(" \"").append(JSONUtil.SCHEMA_TYPE)
					.append(OBJECT_STRING).append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
			try (ResultSet resultSet = md.getColumns(con.getCatalog(), con.getSchema(), objectTypeId, null)) {
				if (enableQuery) {
					sbSchema.append(BACKSLASH).append(SQL_QUERY).append(OPEN_PROPERTIES);
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
					sbSchema.append("},");
					enableQuery = false;
				}
				while (resultSet.next()) {
					getStandardJsonNestedSchema(con, sbSchema, dataTypes, resultSet);
				}
			}

		} catch (SQLException e) {
			throw new ConnectorException(e.getMessage());
		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
		sbSchema.append("}}");
		JsonNode rootNode = null;
		try {
			rootNode = JSONUtil.getDefaultObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
					.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).readTree(sbSchema.toString());
			if (rootNode != null) {
				jsonSchema = JSONUtil.prettyPrintJSON(rootNode);
			}
		} catch (Exception e) {
			throw new ConnectorException(SCHEMA_BUILDER_EXCEPTION, e.getMessage());
		}
		return jsonSchema;
	}

	private static void getStandardJsonNestedSchema(Connection con, StringBuilder sbSchema,
			Map<String, String> dataTypes, ResultSet resultSet) throws SQLException {
		String param = resultSet.getString(COLUMN_NAME);
		if (STRING.equals(dataTypes.get(param)) || TIME.equals(dataTypes.get(param))
				|| DATE.equals(dataTypes.get(param)) || BLOB.equals(dataTypes.get(param))
				|| NVARCHAR.equals(dataTypes.get(param)) || TIMESTAMP.equals(dataTypes.get(param))) {
			sbSchema.append(BACKSLASH).append(param).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
		} else if (INTEGER.equals(dataTypes.get(param)) || DOUBLE.equals(dataTypes.get(param))
				|| FLOAT.equals(dataTypes.get(param)) || LONG.equals(dataTypes.get(param))) {
			sbSchema.append(BACKSLASH).append(param).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
		} else if (BOOLEAN.equals(dataTypes.get(param))) {
			sbSchema.append(BACKSLASH).append(param).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(BOOLEAN).append(BACKSLASH);
		} else {
			ArrayDescriptor array = ArrayDescriptor
					.createDescriptor(resultSet.getString(OracleDatabaseConstants.TYPE_NAME), SchemaBuilderUtil.getUnwrapConnection(con));
			boolean type = QueryBuilderUtil.checkArrayDataType(array);
			if(type)  {
				sbSchema.append(BACKSLASH).append(param).append(OPEN_PROPERTIES);
				sbSchema.append(TYPE_OBJECT);
				sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
				iterateOverVarrays(array, sbSchema, con);
				sbSchema.deleteCharAt(sbSchema.length() - 1);
				sbSchema.append('}');
			} else {
				iterateOverStandardNestedTable(array, sbSchema, con);
			}
		}
		sbSchema.append("},");
	}

	/**
	 * This method will iterate over nested tables and get the column names and
	 * types of the inner table for building schema for all Dynamic Operations.
	 *
	 * @param array    the array
	 * @param sbSchema the sb schema
	 * @param con      the con
	 * @throws SQLException the SQL exception
	 */
	public static void iterateOverNestedTables(ArrayDescriptor array, StringBuilder sbSchema, Connection con)
			throws SQLException {
		StructDescriptor structLevel1 = StructDescriptor.createDescriptor(array.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		for (int i = 1; i <= structLevel1.getMetaData().getColumnCount(); i++) {
			sbSchema.append(BACKSLASH).append(structLevel1.getMetaData().getColumnName(i)).append(OPEN_PROPERTIES);
			boolean ctype = checkCharacterDataType(structLevel1,i);
			boolean ntype = checkNumericDataType(structLevel1, i);
			if (ctype) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			} else if (ntype) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			} else {
				sbSchema.append(TYPE_OBJECT);
				sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
				ArrayDescriptor arrayLevel2 = ArrayDescriptor
						.createDescriptor(structLevel1.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type = QueryBuilderUtil.checkArrayDataType(arrayLevel2);
				if(type)  {
					iterateOverVarrays(arrayLevel2, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append("}");
				} else {
					iterateOverInnerNested(sbSchema, con, arrayLevel2);
				}
			}
			sbSchema.append("},");
		}
	}

	private static void iterateOverInnerNested(StringBuilder sbSchema, Connection con, ArrayDescriptor arrayLevel2)
			throws SQLException {
		StructDescriptor structLevel2 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		for (int j = 1; j <= structLevel2.getMetaData().getColumnCount(); j++) {
			boolean ctype2 = checkCharacterDataType(structLevel2,j);
			boolean ntype2 = checkNumericDataType(structLevel2, j);
			sbSchema.append(BACKSLASH).append(structLevel2.getMetaData().getColumnName(j)).append(OPEN_PROPERTIES);
			if (ctype2) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			} else if (ntype2) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			} else if (structLevel2.getMetaData().getColumnType(j) == 2003) {
				sbSchema.append(TYPE_OBJECT);
				sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
				ArrayDescriptor arrayLevel3 = ArrayDescriptor
						.createDescriptor(structLevel2.getMetaData().getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type3 = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
				if(type3){
					iterateOverVarrays(arrayLevel3, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append('}');
				} else {
					iterateOverNestedTables(arrayLevel3, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append('}');
				}
			}
			sbSchema.append("},");
		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
		sbSchema.append('}');
	}

	/**
	 * This method will build the Json Schema for Stored Procedure based on the
	 * Input Parameters and its DataTypes.
	 *
	 * @param con          the con
	 * @param objectTypeId the object type id
	 * @param params       the params
	 * @return the procedure schema
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings({"java:S3776"})
	public static String getProcedureSchema(Connection con, String objectTypeId, List<String> params)
			throws SQLException {
		String jsonSchema = null;
		StringBuilder sbSchema = new StringBuilder();
		Map<String, Integer> dataTypes = null;
		boolean nested = false;
		boolean obj = false;
		boolean refIn = false;
		boolean diffdatatype = true;
		String packageName = SchemaBuilderUtil.getProcedurePackageName(objectTypeId);
		dataTypes = new ProcedureMetaDataUtil(con, objectTypeId, packageName).getDataType();
		if (!params.isEmpty()) {
			sbSchema.append("{");
			sbSchema.append(JSON_DRAFT4_DEFINITION);
			sbSchema.append(" \"").append(JSONUtil.SCHEMA_TYPE).append(OBJECT_STRING);
			sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
			for (String param : params) {
				sbSchema.append(BACKSLASH).append(param).append(OPEN_PROPERTIES);
				if (dataTypes.get(param).equals(12) 
						|| dataTypes.get(param).equals(-1) || dataTypes.get(param).equals(1)
						|| dataTypes.get(param).equals(2005) || dataTypes.get(param).equals(2009)
						|| dataTypes.get(param).equals(-9) || dataTypes.get(param).equals(1111)
						|| dataTypes.get(param).equals(123) || dataTypes.get(param).equals(-15)
						|| dataTypes.get(param).equals(-16) || dataTypes.get(param).equals(-2)
						|| dataTypes.get(param).equals(-3) || dataTypes.get(param).equals(-4)
						|| dataTypes.get(param).equals(2004) || dataTypes.get(param).equals(91)||dataTypes.get(param).equals(92)
						||dataTypes.get(param).equals(93)) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
					nested = false;
					obj= false;
					refIn = false;
				}
				else if (dataTypes.get(param).equals(4) || dataTypes.get(param).equals(2)
						|| dataTypes.get(param).equals(-5) || dataTypes.get(param).equals(3)
						|| dataTypes.get(param).equals(8) || dataTypes.get(param).equals(6)
						|| dataTypes.get(param).equals(7) || dataTypes.get(param).equals(-6)
						|| dataTypes.get(param).equals(5)) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
					nested = false;
					obj= false;
					refIn = false;
				} else if (dataTypes.get(param).equals(16) || dataTypes.get(param).equals(-7)) {
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(BOOLEAN).append(BACKSLASH);
					nested = false;
					obj= false;
					refIn = false;
				}
				 else if (dataTypes.get(param).equals(2012)) {
					sbSchema.replace(sbSchema.lastIndexOf(",")+1, sbSchema.lastIndexOf(""), "");
					nested = false;
					refIn = true;
					obj= false;
					}
				else if (dataTypes.get(param).equals(2003)) {
					ArrayDescriptor array = ArrayDescriptor
							.createDescriptor(QueryBuilderUtil.getTypeName(con, objectTypeId, param), SchemaBuilderUtil.getUnwrapConnection(con));
					sbSchema.append(TYPE_OBJECT);
					sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
					iterateOverVarrays(array, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append("}");
					nested = false;
					obj= false;
					refIn = false;
				}
				
				else if (dataTypes.get(param).equals(2002)) {
					StructDescriptor structDesc = StructDescriptor.createDescriptor
                            (QueryBuilderUtil.getTypeName(con, objectTypeId, param), SchemaBuilderUtil.getUnwrapConnection(con));
					ResultSetMetaData md = structDesc.getMetaData();
					sbSchema.append(TYPE_OBJECT);
					sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
					for (int i = 1; i <= md.getColumnCount(); i++) {
					if(NUMBER.equalsIgnoreCase(md.getColumnTypeName(i))
							||FLOAT.equalsIgnoreCase(md.getColumnTypeName(i))
							||INTEGER.equalsIgnoreCase(md.getColumnTypeName(i))
							||SMALLINT.equalsIgnoreCase(md.getColumnTypeName(i))
							||DOUBLE.equalsIgnoreCase(md.getColumnTypeName(i))
							||DECIMAL.equalsIgnoreCase(md.getColumnTypeName(i))
							||RAW.equalsIgnoreCase(md.getColumnTypeName(i))
							||LONG.equalsIgnoreCase(md.getColumnTypeName(i))){
						sbSchema.append(BACKSLASH).append(md.getColumnName(i)).append(OPEN_PROPERTIES);
						sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
						sbSchema.append("},");
						obj= true;
						nested = false;
						refIn = false;
				}
					else if(VARCHAR.equalsIgnoreCase(md.getColumnTypeName(i))
							||DATE.equalsIgnoreCase(md.getColumnTypeName(i))
							||CHAR.equalsIgnoreCase(md.getColumnTypeName(i))
							||NCHAR.equalsIgnoreCase(md.getColumnTypeName(i))
							||NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(i))
							||BLOB.equalsIgnoreCase(md.getColumnTypeName(i))
							||CLOB.equalsIgnoreCase(md.getColumnTypeName(i))
							||NCLOB.equalsIgnoreCase(md.getColumnTypeName(i))
							||TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(i))
							||LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(i))){
						sbSchema.append(BACKSLASH).append(md.getColumnName(i)).append(OPEN_PROPERTIES);
						sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
						sbSchema.append("},");
						obj= true;
						nested = false;
						refIn = false;
				}
					}
				}
				
				else if (dataTypes.get(param).equals(2010)) {
					ArrayDescriptor array = ArrayDescriptor
							.createDescriptor(QueryBuilderUtil.getTypeName(con, objectTypeId, param), SchemaBuilderUtil.getUnwrapConnection(con));
					
					if (array.getBaseName().equals(VARCHAR) || array.getBaseName().equalsIgnoreCase(DATE)
							|| array.getBaseName().equalsIgnoreCase(CHAR) || array.getBaseName().equalsIgnoreCase(NCHAR)
							|| array.getBaseName().equalsIgnoreCase(NVARCHAR)
							|| array.getBaseName().equalsIgnoreCase(BLOB) || array.getBaseName().equalsIgnoreCase(CLOB)
							|| array.getBaseName().equalsIgnoreCase(NCLOB)
							|| array.getBaseName().equalsIgnoreCase(TIMESTAMP)
							|| array.getBaseName().equalsIgnoreCase(LONGVARCHAR)) {
						nested = false;
						sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS).append("\"type\": \"string\"\r\n")
								.append(CLOSE_ARRAY);
						diffdatatype = false;
						obj= false;
						refIn = false;
					} else if (array.getBaseName().equalsIgnoreCase(NUMBER)
							|| array.getBaseName().equalsIgnoreCase(FLOAT)
							|| array.getBaseName().equalsIgnoreCase(INTEGER)
							|| array.getBaseName().equalsIgnoreCase(SMALLINT)
							|| array.getBaseName().equalsIgnoreCase(NUMERIC)
							|| array.getBaseName().equalsIgnoreCase(DOUBLE)
							|| array.getBaseName().equalsIgnoreCase(DECIMAL)) {
						nested = false;
						obj= false;
						refIn = false;
						sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS).append("\"type\": \"integer\"\r\n")
								.append(CLOSE_ARRAY);
						diffdatatype = false;
					} else if (array.getBaseType() == 2002) {
						nested = true;
						obj= false;
						refIn = false;
						sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS);
						sbSchema.append(TYPE_OBJECT);
						sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
 						iterateOverProcedureNestedTables(array, sbSchema, con);
						sbSchema.deleteCharAt(sbSchema.length() - 1);
						sbSchema.append('}');
					}
					if (diffdatatype) {
						sbSchema.deleteCharAt(sbSchema.length() - 1);
						sbSchema.append("}");
					}
				}
				
				if(obj) {

					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append("}");
				
				}
				
				if (nested) {
					sbSchema.append(CLOSE_ARRAY);
				}
				
				if(!refIn) {
				sbSchema.append("},");
				}
				
			}
			sbSchema.deleteCharAt(sbSchema.length() - 1);
			sbSchema.append("}}");
			JsonNode rootNode = null;
			try {
				rootNode = JSONUtil.getDefaultObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
						.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS).readTree(sbSchema.toString());
				if (rootNode != null) {
					jsonSchema = JSONUtil.prettyPrintJSON(rootNode);
				}
			} catch (Exception e) {
				throw new ConnectorException(SCHEMA_BUILDER_EXCEPTION, e.getMessage());
			}
		}
		return jsonSchema;
	}

	/**
	 * This method will get the Json Schema for Dynamic Update, Stored procedure and
	 * Dynamic Delete Response.
	 *
	 * @param opsType the ops type
	 * @return json
	 */
	public static String getQueryJsonSchema(String opsType) {
		ObjectMapper mapper = new ObjectMapper().disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
				.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		String json = null;
		try {
			SchemaFactoryWrapper wrapper = new SchemaFactoryWrapper();
			if (opsType.equals(DYNAMIC_UPDATE)) {
				mapper.acceptJsonFormatVisitor(UpdatePojo.class, wrapper);
			} else if (opsType.equals(DYNAMIC_DELETE)) {
				mapper.acceptJsonFormatVisitor(DeletePojo.class, wrapper);
			} else {
				mapper.acceptJsonFormatVisitor(QueryResponse.class, wrapper);
			}
			JsonSchema schema = wrapper.finalSchema();
			json = JSONUtil.prettyPrintJSON(schema);
		} catch (Exception e) {
			throw new ConnectorException("Failed to build Schema", e);
		}
		return json;
	}

	/**
	 * This method will iterate over varray elements and build the schema
	 * accordingly.
	 *
	 * @param array    the array
	 * @param sbSchema the sb schema
	 * @param con 
	 */
	public static void iterateOverVarrays(ArrayDescriptor array, StringBuilder sbSchema, Connection con) {
		boolean obj = false;
		try {
			for (int i = 1; i <= array.getMaxLength(); i++) {
				
				if (array.getBaseName().equals(VARCHAR) || array.getBaseName().equalsIgnoreCase(DATE)
						|| array.getBaseName().equalsIgnoreCase(TIME) || array.getBaseName().equalsIgnoreCase(CHAR)
						|| array.getBaseName().equalsIgnoreCase(NCHAR) || array.getBaseName().equalsIgnoreCase(NVARCHAR)
						|| array.getBaseName().equalsIgnoreCase(TIMESTAMP)
						|| array.getBaseName().equalsIgnoreCase(LONGVARCHAR)) {
					sbSchema.append(BACKSLASH + ELEMENT + i + OPEN_PROPERTIES);
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
					obj = false;
				} else if (array.getBaseName().equals(NUMBER) || array.getBaseName().equalsIgnoreCase(FLOAT)
						|| array.getBaseName().equalsIgnoreCase(INTEGER)
						|| array.getBaseName().equalsIgnoreCase(TINYINT)
						|| array.getBaseName().equalsIgnoreCase(SMALLINT)
						|| array.getBaseName().equalsIgnoreCase(DOUBLE) || array.getBaseName().equalsIgnoreCase(DECIMAL)
						|| array.getBaseName().equalsIgnoreCase(NUMERIC)) {
					sbSchema.append(BACKSLASH + ELEMENT + i + OPEN_PROPERTIES);
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
					obj = false;
				}
				else {
					obj = iterateOverInnerVarrays(array, sbSchema, con, i);
				}
				if(obj) {

					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append("}");
				}
				sbSchema.append("},");
			}
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}

	private static boolean iterateOverInnerVarrays(ArrayDescriptor array, StringBuilder sbSchema, Connection con, int i)
			throws SQLException {
		boolean obj;
		StructDescriptor structDesc = StructDescriptor.createDescriptor(array.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		obj = true;
		sbSchema.append(BACKSLASH + ELEMENT + i + OPEN_PROPERTIES);
		sbSchema.append(TYPE_OBJECT);
		sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
		for (int l = 1; l <= md.getColumnCount(); l++) {
		if(md.getColumnTypeName(l).equals(VARCHAR) || md.getColumnTypeName(l).equalsIgnoreCase(DATE)
				|| md.getColumnTypeName(l).equalsIgnoreCase(TIME) || md.getColumnTypeName(l).equalsIgnoreCase(CHAR)
				|| md.getColumnTypeName(l).equalsIgnoreCase(NCHAR) || md.getColumnTypeName(l).equalsIgnoreCase(NVARCHAR)
				|| md.getColumnTypeName(l).equalsIgnoreCase(TIMESTAMP)
				|| md.getColumnTypeName(l).equalsIgnoreCase(LONGVARCHAR)) {
			sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			sbSchema.append("}");
		}
		else if(md.getColumnTypeName(l).equals(NUMBER) || md.getColumnTypeName(l).equalsIgnoreCase(FLOAT)
				|| md.getColumnTypeName(l).equalsIgnoreCase(INTEGER)
				|| md.getColumnTypeName(l).equalsIgnoreCase(TINYINT)
				|| md.getColumnTypeName(l).equalsIgnoreCase(SMALLINT)
				|| md.getColumnTypeName(l).equalsIgnoreCase(DOUBLE) || md.getColumnTypeName(l).equalsIgnoreCase(DECIMAL)
				|| md.getColumnTypeName(l).equalsIgnoreCase(NUMERIC)) {
			sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			sbSchema.append("}");
		}
		sbSchema.append(",");
		}
		return obj;
	}

	/**
	 * This method will iterate over nested tables and get the column names and
	 * types of the inner table for building schema for all Dynamic Operations.
	 *
	 * @param array    the array
	 * @param sbSchema the sb schema
	 * @param con      the con
	 * @throws SQLException the SQL exception
	 */
	@SuppressWarnings({"java:S3776"})
	public static void iterateOverProcedureNestedTables(ArrayDescriptor array, StringBuilder sbSchema, Connection con)
			throws SQLException {
		boolean innerTble = false;
		boolean nestedinnerTble = false;
		boolean nestedinnerObj = false;
		boolean obj = false;
		StructDescriptor structLevel1 = StructDescriptor.createDescriptor(array.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		for (int i = 1; i <= structLevel1.getMetaData().getColumnCount(); i++) {
			sbSchema.append(BACKSLASH).append(structLevel1.getMetaData().getColumnName(i)).append(OPEN_PROPERTIES);
			boolean dtype = checkCharacterDataType(structLevel1,i);
			boolean dtype1 = checkNumericDataType(structLevel1, i);
			if (dtype) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
				innerTble = false;
				obj = false;
			} else if (dtype1) {
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
				innerTble = false;
				obj = false;
			} else if(structLevel1.getMetaData().getColumnType(i) == 2002) {
				StructDescriptor structDesc = StructDescriptor.createDescriptor(structLevel1.getOracleTypeADT().getAttributeType(i), SchemaBuilderUtil.getUnwrapConnection(con));
				ResultSetMetaData md = structDesc.getMetaData();
				sbSchema.append(TYPE_OBJECT);
				sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
				for (int l = 1; l <= md.getColumnCount(); l++) {
				if(NUMBER.equalsIgnoreCase(md.getColumnTypeName(l))
						||FLOAT.equalsIgnoreCase(md.getColumnTypeName(l))
						||INTEGER.equalsIgnoreCase(md.getColumnTypeName(l))
						||SMALLINT.equalsIgnoreCase(md.getColumnTypeName(l))
						||DOUBLE.equalsIgnoreCase(md.getColumnTypeName(l))
						||DECIMAL.equalsIgnoreCase(md.getColumnTypeName(l))
						||RAW.equalsIgnoreCase(md.getColumnTypeName(l))
						||LONG.equalsIgnoreCase(md.getColumnTypeName(l))){
					sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
					sbSchema.append("},");
					obj = true;
					innerTble = false;
			}
				else if(VARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
						||DATE.equalsIgnoreCase(md.getColumnTypeName(l))
						||CHAR.equalsIgnoreCase(md.getColumnTypeName(l))
						||NCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
						||NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
						||BLOB.equalsIgnoreCase(md.getColumnTypeName(l))
						||CLOB.equalsIgnoreCase(md.getColumnTypeName(l))
						||NCLOB.equalsIgnoreCase(md.getColumnTypeName(l))
						||TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(l))
						||LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))){
					sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
					sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
					sbSchema.append("},");
					obj = true;
					innerTble = false;
			}
				}
			
			}
			else {
				ArrayDescriptor arrayLevel2 = ArrayDescriptor
						.createDescriptor(structLevel1.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type1 = QueryBuilderUtil.checkArrayDataType(arrayLevel2);
				obj = false;
				if(type1){
					innerTble = false;
					sbSchema.append(TYPE_OBJECT);
					sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
					iterateOverVarrays(arrayLevel2, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append("}");
				} else {
					StructDescriptor structLevel2 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
					innerTble = true;
					sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS);
					sbSchema.append(TYPE_OBJECT);
					sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
					for (int j = 1; j <= structLevel2.getMetaData().getColumnCount(); j++) {
						sbSchema.append(BACKSLASH).append(structLevel2.getMetaData().getColumnName(j)).append(OPEN_PROPERTIES);
						boolean ctype = checkCharacterDataType(structLevel2,j);
						boolean ntype = checkNumericDataType(structLevel2, j);
						if (ctype) {
							sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
							nestedinnerTble = false;
							nestedinnerObj = false;
						} else if (ntype) {
							sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
							nestedinnerTble = false;
							nestedinnerObj = false;
						} else if(structLevel2.getMetaData().getColumnType(j) == 2002) {
							StructDescriptor structDesc = StructDescriptor.createDescriptor(structLevel2.getOracleTypeADT().getAttributeType(j), SchemaBuilderUtil.getUnwrapConnection(con));
							ResultSetMetaData md = structDesc.getMetaData();
							sbSchema.append("\"type\": \"object\",");
							sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
							for (int l = 1; l <= md.getColumnCount(); l++) {
							if(NUMBER.equalsIgnoreCase(md.getColumnTypeName(l))
									||FLOAT.equalsIgnoreCase(md.getColumnTypeName(l))
									||INTEGER.equalsIgnoreCase(md.getColumnTypeName(l))
									||SMALLINT.equalsIgnoreCase(md.getColumnTypeName(l))
									||DOUBLE.equalsIgnoreCase(md.getColumnTypeName(l))
									||DECIMAL.equalsIgnoreCase(md.getColumnTypeName(l))
									||RAW.equalsIgnoreCase(md.getColumnTypeName(l))
									||LONG.equalsIgnoreCase(md.getColumnTypeName(l))){
								sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
								sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
								sbSchema.append("},");
								nestedinnerObj = true;
								nestedinnerTble = false;
						}
							else if(VARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
									||DATE.equalsIgnoreCase(md.getColumnTypeName(l))
									||CHAR.equalsIgnoreCase(md.getColumnTypeName(l))
									||NCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
									||NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
									||BLOB.equalsIgnoreCase(md.getColumnTypeName(l))
									||CLOB.equalsIgnoreCase(md.getColumnTypeName(l))
									||NCLOB.equalsIgnoreCase(md.getColumnTypeName(l))
									||TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(l))
									||LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))){
								sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
								sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
								sbSchema.append("},");
								nestedinnerObj = true;
								nestedinnerTble = false;
						}
							}
						} 
						else if (structLevel2.getMetaData().getColumnType(j) == 2003) {
							nestedinnerTble = true;
							nestedinnerObj = false;
							sbSchema.append(OPEN_ARRAY).append(OPEN_ITEMS);
							sbSchema.append(TYPE_OBJECT);
							sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
							ArrayDescriptor arrayLevel3 = ArrayDescriptor
									.createDescriptor(structLevel2.getMetaData().getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
							boolean type2 = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
							if(type2){
								iterateOverVarrays(arrayLevel3, sbSchema, con);
								sbSchema.deleteCharAt(sbSchema.length() - 1);
								sbSchema.append('}');
							} else {
								iterateOverProcedureNestedTables(arrayLevel3, sbSchema, con);
								sbSchema.deleteCharAt(sbSchema.length() - 1);
								sbSchema.append('}');
							}
						}
						if(nestedinnerObj) {
							sbSchema.deleteCharAt(sbSchema.length() - 1);
							sbSchema.append("}");
						}
						if (nestedinnerTble) {
							sbSchema.append(CLOSE_ARRAY);
						}
						sbSchema.append("},");
					}
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append('}');
				}
			}
			appendCloseOnProcNestedTables(sbSchema, innerTble, obj);
			sbSchema.append("},");
		}
	}

	private static void appendCloseOnProcNestedTables(StringBuilder sbSchema, boolean innerTble, boolean obj) {
		if (innerTble) {
			sbSchema.append(CLOSE_ARRAY);
		}
		if(obj) {
			sbSchema.deleteCharAt(sbSchema.length() - 1);
			sbSchema.append("}");
		}
	}

	/**
	 * 
	 * @param structLevel1
	 * @param obj
	 * @param sbSchema
	 * @param con
	 * @param i
	 * @throws SQLException
	 */
	public static void iterateOverProcedureObject(StructDescriptor structLevel1 ,StringBuilder sbSchema, Connection con,int i)
			throws SQLException {
		StructDescriptor structDesc = StructDescriptor.createDescriptor(structLevel1.getOracleTypeADT().getAttributeType(i), SchemaBuilderUtil.getUnwrapConnection(con));
		ResultSetMetaData md = structDesc.getMetaData();
		sbSchema.append("\"type\": \"object\",");
		sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
		for (int l = 1; l <= md.getColumnCount(); l++) {
		if(NUMBER.equalsIgnoreCase(md.getColumnTypeName(l))
				||FLOAT.equalsIgnoreCase(md.getColumnTypeName(l))
				||INTEGER.equalsIgnoreCase(md.getColumnTypeName(l))
				||SMALLINT.equalsIgnoreCase(md.getColumnTypeName(l))
				||DOUBLE.equalsIgnoreCase(md.getColumnTypeName(l))
				||DECIMAL.equalsIgnoreCase(md.getColumnTypeName(l))
				||RAW.equalsIgnoreCase(md.getColumnTypeName(l))
				||LONG.equalsIgnoreCase(md.getColumnTypeName(l))){
			sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			sbSchema.append("},");
	}
		else if(VARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
				||DATE.equalsIgnoreCase(md.getColumnTypeName(l))
				||CHAR.equalsIgnoreCase(md.getColumnTypeName(l))
				||NCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
				||NVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))
				||BLOB.equalsIgnoreCase(md.getColumnTypeName(l))
				||CLOB.equalsIgnoreCase(md.getColumnTypeName(l))
				||NCLOB.equalsIgnoreCase(md.getColumnTypeName(l))
				||TIMESTAMP.equalsIgnoreCase(md.getColumnTypeName(l))
				||LONGVARCHAR.equalsIgnoreCase(md.getColumnTypeName(l))){
			sbSchema.append(BACKSLASH).append(md.getColumnName(l)).append(OPEN_PROPERTIES);
			sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			sbSchema.append("},");
	}
		}
	}
	
	/**
	 * This method will iterate over nested tables and get the column names and
	 * types of the inner table for building schema for all Standard Operations.
	 *
	 * @param array    the array
	 * @param sbSchema the sb schema
	 * @param con      the con
	 * @throws SQLException the SQL exception
	 */
	public static void iterateOverStandardNestedTable(ArrayDescriptor array, StringBuilder sbSchema, Connection con)
			throws SQLException {
		StructDescriptor structLevel1 = StructDescriptor.createDescriptor(array.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		for (int i = 1; i <= structLevel1.getMetaData().getColumnCount(); i++) {
			boolean ctype = checkCharacterDataType(structLevel1,i);
			boolean ntype = checkNumericDataType(structLevel1, i);
			if (ctype) {
				sbSchema.append(BACKSLASH).append(structLevel1.getMetaData().getColumnName(i)).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			} else if (ntype) {
				sbSchema.append(BACKSLASH).append(structLevel1.getMetaData().getColumnName(i)).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			} else {
				ArrayDescriptor arrayLevel2 = ArrayDescriptor
						.createDescriptor(structLevel1.getMetaData().getColumnTypeName(i), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type2 = QueryBuilderUtil.checkArrayDataType(arrayLevel2);
				if(type2) {
					sbSchema.append(BACKSLASH).append(structLevel1.getMetaData().getColumnName(i)).append(OPEN_PROPERTIES);
					sbSchema.append(TYPE_OBJECT);
					sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
					iterateOverVarrays(arrayLevel2, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append('}');
				} else {
					iterateOverInnerNestedValue(sbSchema, con, arrayLevel2);
				}
			}
			sbSchema.append("},");

		}
		sbSchema.deleteCharAt(sbSchema.length() - 1);
	}

	/**
	 * 
	 * @param sbSchema
	 * @param con
	 * @param arrayLevel2
	 * @throws SQLException
	 */
	private static void iterateOverInnerNestedValue(StringBuilder sbSchema, Connection con, ArrayDescriptor arrayLevel2)
			throws SQLException {
		StructDescriptor structLevel2 = StructDescriptor.createDescriptor(arrayLevel2.getBaseName(), SchemaBuilderUtil.getUnwrapConnection(con));
		for (int j = 1; j <= structLevel2.getMetaData().getColumnCount(); j++) {
			if (structLevel2.getMetaData().getColumnType(j) == 12
					|| structLevel2.getMetaData().getColumnType(j) == 91
					|| structLevel2.getMetaData().getColumnType(j) == 1
					|| structLevel2.getMetaData().getColumnType(j) == 92
					|| structLevel2.getMetaData().getColumnType(j) == 93
					|| structLevel2.getMetaData().getColumnType(j) == -1
					|| structLevel2.getMetaData().getColumnType(j) == 2005
					|| structLevel2.getMetaData().getColumnType(j) == 2009
					|| structLevel2.getMetaData().getColumnType(j) == -9
					|| structLevel2.getMetaData().getColumnType(j) == 1111
					|| structLevel2.getMetaData().getColumnType(j) == 123
					|| structLevel2.getMetaData().getColumnType(j) == -15
					|| structLevel2.getMetaData().getColumnType(j) == -16
					|| structLevel2.getMetaData().getColumnType(j) == -2
					|| structLevel2.getMetaData().getColumnType(j) == -3
					|| structLevel2.getMetaData().getColumnType(j) == -4
					|| structLevel2.getMetaData().getColumnType(j) == 2004) {
				sbSchema.append(BACKSLASH).append(structLevel2.getMetaData().getColumnName(j)).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(STRING).append(BACKSLASH);
			} else if (structLevel2.getMetaData().getColumnType(j) == 2
					|| structLevel2.getMetaData().getColumnType(j) == 4
					|| structLevel2.getMetaData().getColumnType(j) == -5
					|| structLevel2.getMetaData().getColumnType(j) == 3
					|| structLevel2.getMetaData().getColumnType(j) == 8
					|| structLevel2.getMetaData().getColumnType(j) == 6
					|| structLevel2.getMetaData().getColumnType(j) == 7
					|| structLevel2.getMetaData().getColumnType(j) == -6
					|| structLevel2.getMetaData().getColumnType(j) == 5) {
				sbSchema.append(BACKSLASH).append(structLevel2.getMetaData().getColumnName(j)).append(OPEN_PROPERTIES);
				sbSchema.append(BACKSLASH).append(JSONUtil.SCHEMA_TYPE).append(DOUBLE_BACKSLASH).append(INTEGER).append(BACKSLASH);
			} else if (structLevel2.getMetaData().getColumnType(j) == 2003) {
				sbSchema.append(BACKSLASH).append(structLevel2.getMetaData().getColumnName(j)).append(OPEN_PROPERTIES);
				sbSchema.append(TYPE_OBJECT);
				sbSchema.append(" \"").append(JSONUtil.SCHEMA_PROPERTIES).append(OPEN_PROPERTIES);
				ArrayDescriptor arrayLevel3 = ArrayDescriptor
						.createDescriptor(structLevel2.getMetaData().getColumnTypeName(j), SchemaBuilderUtil.getUnwrapConnection(con));
				boolean type3 = QueryBuilderUtil.checkArrayDataType(arrayLevel3);
				if(type3) {
					iterateOverVarrays(arrayLevel3, sbSchema, con);
					sbSchema.deleteCharAt(sbSchema.length() - 1);
					sbSchema.append('}');
				} else {
					throw new ConnectorException("Nested table level exhausted!!!");
				}

			}
			sbSchema.append("},");
		}

		sbSchema.deleteCharAt(sbSchema.length() - 1);
	}

	/**
	 * Get the procedure name from the Object ID
	 * 
	 * @param objectTypeId
	 * @return the procedure name
	 */
	public static String getProcedureName(String objectTypeId) {
		String procedure = null;
		if (objectTypeId != null && objectTypeId.lastIndexOf(".") > 0) {
			procedure = objectTypeId.substring(objectTypeId.lastIndexOf(".") + 1, objectTypeId.length());
		} else
			procedure = objectTypeId;
		return procedure;
	}

	/**
	 * Get the package name from the procedure Object ID
	 * 
	 * @param objectTypeId
	 * @return the package name
	 */
	public static String getProcedurePackageName(String objectTypeId) {

		String procedurePackage = null;
		if (objectTypeId != null && objectTypeId.lastIndexOf(".") > 0) {
			procedurePackage = objectTypeId.substring(0, objectTypeId.lastIndexOf("."));
		} else
			procedurePackage = null;
		return procedurePackage;
	}

	
	private static boolean checkCharacterDataType(StructDescriptor structLevel1, int i) throws SQLException{
		return structLevel1.getMetaData().getColumnType(i) == 12 || structLevel1.getMetaData().getColumnType(i) == 91
				|| structLevel1.getMetaData().getColumnType(i) == 92
				|| structLevel1.getMetaData().getColumnType(i) == 1
				|| structLevel1.getMetaData().getColumnType(i) == 93
				|| structLevel1.getMetaData().getColumnType(i) == -1
				|| structLevel1.getMetaData().getColumnType(i) == 2005
				|| structLevel1.getMetaData().getColumnType(i) == 2009
				|| structLevel1.getMetaData().getColumnType(i) == -9
				|| structLevel1.getMetaData().getColumnType(i) == 1111
				|| structLevel1.getMetaData().getColumnType(i) == 123
				|| structLevel1.getMetaData().getColumnType(i) == -15
				|| structLevel1.getMetaData().getColumnType(i) == -16
				|| structLevel1.getMetaData().getColumnType(i) == -2
				|| structLevel1.getMetaData().getColumnType(i) == -3
				|| structLevel1.getMetaData().getColumnType(i) == -4
				|| structLevel1.getMetaData().getColumnType(i) == 2004;
	}
		
		private static boolean checkNumericDataType(StructDescriptor structLevel1, int i) throws SQLException{
			return structLevel1.getMetaData().getColumnType(i) == 2
					|| structLevel1.getMetaData().getColumnType(i) == 4
					|| structLevel1.getMetaData().getColumnType(i) == 3
					|| structLevel1.getMetaData().getColumnType(i) == -5
					|| structLevel1.getMetaData().getColumnType(i) == 8
					|| structLevel1.getMetaData().getColumnType(i) == 6
					|| structLevel1.getMetaData().getColumnType(i) == 7
					|| structLevel1.getMetaData().getColumnType(i) == -6
					|| structLevel1.getMetaData().getColumnType(i) == 5;
			}
		
		
		
		/**
		 * 
		 * @param con
		 * @return
		 */
		public static Connection getUnwrapConnection(Connection con){
			try {
				return  con.unwrap(OracleConnection.class);
			} catch (SQLException e) {
			if(e.getMessage().contains("Object does not wrap anything\r\n" + 
					"with requested interface")) {
				return con;
			}
			}
			return con;
			
		}
		
	
	/**
	 * Gets the table names.
	 *
	 * @param objectTypeId the object type id
	 * @return the table names
	 */
	public static String[] getTableNames(String objectTypeId) {
		if (objectTypeId.contains(",")) {
			return objectTypeId.split("[,]", 0);
		} else {
			return new String[] { objectTypeId };
		}
	}
}
