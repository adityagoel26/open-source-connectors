// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;
import com.boomi.connector.databaseconnector.model.DeletePojo;
import com.boomi.connector.databaseconnector.model.QueryResponse;
import com.boomi.connector.databaseconnector.model.UpdatePojo;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.factories.SchemaFactoryWrapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * This Util Class will Generate the JSON Schema by taking each column name from
 * the table and associated DataTypes of the column. Based on the column
 * datatype the Schema Type will be either String, Boolean or Number.
 *
 * @author swastik.vn
 */
public class SchemaBuilderUtil {

    private static final String BACKSLASH_COLON = "\": \"";

    private static final String BACKSLASH_OBJECT = "\": \"object\",";

    private static final String BACKSLASH_BRACKET = "\": {";

    /**
     * Instantiates a new schema builder util.
     */
    private SchemaBuilderUtil() {

    }

    /**
     * Build Json Schema Based on the Database Column names and column types.
     *
     * @param sqlConnection the _sqlConnection
     * @param objectTypeId  the object type id
     * @param enableQuery   the enable query
     * @param output        the output
     * @param schemaName    the schema name
     * @return the json schema
     * @throws SQLException
     */
    public static String getJsonSchema(Connection sqlConnection, String objectTypeId, boolean enableQuery,
            boolean output, boolean isBatching, String schemaName) throws SQLException {

        String jsonSchema = null;
        StringBuilder sbSchema = new StringBuilder();
        String databaseName = sqlConnection.getMetaData().getDatabaseProductName();
        Map<String, String> dataTypes = new HashMap<>();
        String[] tableNames = getTableNames(objectTypeId);
        try {
            if (objectTypeId.contains(",")) {
                for (String tableName : tableNames) {
                    dataTypes.putAll(MetadataExtractor.getDataTypesWithTable(sqlConnection, tableName.trim()));
                }
            } else {
                dataTypes.putAll(new MetadataExtractor(sqlConnection, objectTypeId, schemaName).getDataType());
            }
            DatabaseMetaData md = sqlConnection.getMetaData();
            if (isBatching) {
                sbSchema.append("{").append(DatabaseConnectorConstants.JSON_DRAFT4_DEFINITION).append(" \"").append(
                                JSONUtil.SCHEMA_TYPE).append("\": \"array\",").append(" \"").append("items\":{")
                                .append(" \"").append(JSONUtil.SCHEMA_TYPE).append(BACKSLASH_OBJECT).append(" \"")
                                .append(JSONUtil.SCHEMA_PROPERTIES).append(BACKSLASH_BRACKET);
            } else {
                sbSchema.append("{").append(DatabaseConnectorConstants.JSON_DRAFT4_DEFINITION).append(" \"").append(
                                JSONUtil.SCHEMA_TYPE).append(BACKSLASH_OBJECT).append(" \"")
                                .append(JSONUtil.SCHEMA_PROPERTIES).append(BACKSLASH_BRACKET);
            }
            for (String tableName : tableNames) {
                if (DatabaseConnectorConstants.ORACLE.equals(databaseName) && tableName.contains("/")) {
                    tableName = tableName.replace("/", "//");
                } else if ((DatabaseConnectorConstants.MYSQL.equals(databaseName)
                        || DatabaseConnectorConstants.POSTGRESQL.equals(databaseName)) && tableName.contains("\\")) {
                    tableName = tableName.replace("\\", "\\\\");
                }
                try (ResultSet resultSet = md.getColumns(sqlConnection.getCatalog(), schemaName, tableName.trim(),
                        null);) {
                    while (resultSet.next()) {
                        appendSchemaNodes(objectTypeId, output, tableName, resultSet, sbSchema, dataTypes);
                    }
                    if (enableQuery) {
                        sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(
                                DatabaseConnectorConstants.SQL_QUERY).append(BACKSLASH_BRACKET);
                        sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE).append(
                                BACKSLASH_COLON).append(DatabaseConnectorConstants.STRING).append(
                                DatabaseConnectorConstants.DOUBLE_QUOTE);
                        sbSchema.append("},");
                    }
                }
            }
        } catch (SQLException e) {
            throw new ConnectorException(e);
        }

        return closeAndStringifyJsonSchema(isBatching, sbSchema, jsonSchema);
    }

    /**
     * Handles the appending of column names and data types to the schema string
     * for the given object type ID, table name, and result set.
     *
     * @param objectTypeId The ID of the object type being processed.
     * @param output       A boolean flag indicating whether to append the column name
     *                     and data type to the schema string.
     * @param tableName    The name of the table being processed.
     * @param resultSet    The result set containing the column information.
     * @param sbSchema     The StringBuilder to which the column names and data types
     *                     will be appended.
     * @param dataTypes    A Map containing the data types for the columns.
     * @throws SQLException
     */
    private static void appendSchemaNodes(String objectTypeId, boolean output, String tableName,
            ResultSet resultSet, StringBuilder sbSchema, Map<String, String> dataTypes) throws SQLException {
        String param = "";
        if (objectTypeId.contains(",")) {
            if (tableName.contains("\\")) {
                throw new ConnectorException("Kindly check the table name!!!");
            }
            param = tableName.trim() + DatabaseConnectorConstants.DOT + resultSet.getString(
                    DatabaseConnectorConstants.COLUMN_NAME);
        } else {
            param = resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME);
        }
        if (output) {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(
                    resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).append(
                    BACKSLASH_BRACKET);
        } else {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(param).append(
                    BACKSLASH_BRACKET);
        }
        if (dataTypes.get(param) == null) {
            throw new SQLException(
                    "The data type " + resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)
                            + " is not supported in the connector!");
        } else if (DatabaseConnectorConstants.STRING.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.TIME.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.DATE.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.JSON.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.NVARCHAR.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.BLOB.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.TIMESTAMP.equals(dataTypes.get(param))) {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE)
                    .append(BACKSLASH_COLON).append(DatabaseConnectorConstants.STRING).append(
                            DatabaseConnectorConstants.DOUBLE_QUOTE);
        } else if (DatabaseConnectorConstants.INTEGER.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.DOUBLE.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.FLOAT.equals(dataTypes.get(param))
                || DatabaseConnectorConstants.LONG.equals(dataTypes.get(param))) {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE)
                    .append(BACKSLASH_COLON).append(DatabaseConnectorConstants.INTEGER).append(
                            DatabaseConnectorConstants.DOUBLE_QUOTE);
        } else if (DatabaseConnectorConstants.BOOLEAN.equals(dataTypes.get(param))) {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE)
                    .append(BACKSLASH_COLON).append(DatabaseConnectorConstants.BOOLEAN).append(
                            DatabaseConnectorConstants.DOUBLE_QUOTE);
        }
        sbSchema.append("},");
    }

    /**
     * Closes and stringifies the JSON schema by appending the closing braces and
     * converting the StringBuilder to a String.
     *
     * @param isBatching A boolean flag indicating whether batching is enabled.
     * @param sbSchema The StringBuilder containing the JSON schema.
     * @param jsonSchema The initial JSON schema string.
     * @return jsonSchema The stringified JSON schema.
     */
    private static String closeAndStringifyJsonSchema(boolean isBatching, StringBuilder sbSchema, String jsonSchema) {
        String json;
        sbSchema.deleteCharAt(sbSchema.length() - 1);
        sbSchema.append("}}");

        if (isBatching) {
            sbSchema.append("}");
        }

        json = sbSchema.toString();
        JsonNode rootNode = null;

        try {
            rootNode = JSONUtil.getDefaultObjectMapper().readTree(json);
            if (rootNode != null) {
                jsonSchema = JSONUtil.prettyPrintJSON(rootNode);
            }
        } catch (Exception e) {
            throw new ConnectorException(DatabaseConnectorConstants.SCHEMA_BUILDER_EXCEPTION, e);
        }

        return jsonSchema;
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

    /**
     * This method will build the Json Schema for Stored Procedure based on the
     * Input Parameters and its DataTypes.
     *
     * @param sqlConnection the _sqlConnection
     * @param objectTypeId  the object type id
     * @param inParams      the in params
     * @param schemaName    the schema name
     * @return the procedure schema
     */
    public static String getProcedureSchema(Connection sqlConnection, String objectTypeId, List<String> inParams,
            String schemaName) {
        String jsonSchema = null;
        StringBuilder sbSchema = new StringBuilder();
        Map<String, Integer> dataTypes = null;
        String procedure = SchemaBuilderUtil.getProcedureName(objectTypeId);
        String packageName = SchemaBuilderUtil.getProcedurePackageName(objectTypeId);
        dataTypes = ProcedureMetaDataUtil.getProcedureMetadata(sqlConnection, procedure, packageName, schemaName);

        if (inParams.isEmpty()) {
            return null;
        }

        sbSchema.append("{").append(DatabaseConnectorConstants.JSON_DRAFT4_DEFINITION).append(" \"").append(
                        JSONUtil.SCHEMA_TYPE).append(BACKSLASH_OBJECT).append(" \"")
                        .append(JSONUtil.SCHEMA_PROPERTIES).append(BACKSLASH_BRACKET);

        for (String param : inParams) {
            sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(param).append(BACKSLASH_BRACKET);
            if (null == dataTypes.get(param)) {
                throw new ConnectorException("IN/OUT param is not supported in connector!!!");
            }
            if (dataTypes.get(param).equals(Types.VARCHAR) || dataTypes.get(param).equals(Types.TIME)
                    || dataTypes.get(param).equals(Types.DATE) || dataTypes.get(param).equals(Types.LONGVARCHAR)
                    || dataTypes.get(param).equals(Types.CLOB) || dataTypes.get(param).equals(Types.NVARCHAR)
                    || dataTypes.get(param).equals(Types.OTHER) || dataTypes.get(param).equals(123)
                    || dataTypes.get(param).equals(Types.TIMESTAMP) || dataTypes.get(param).equals(Types.CHAR)
                    || dataTypes.get(param).equals(Types.NCHAR) || dataTypes.get(param).equals(Types.LONGNVARCHAR)
                    || dataTypes.get(param).equals(Types.BINARY) || dataTypes.get(param).equals(Types.VARBINARY)
                    || dataTypes.get(param).equals(Types.LONGVARBINARY) || dataTypes.get(param).equals(Types.BLOB)){
                sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE)
                        .append(BACKSLASH_COLON).append(DatabaseConnectorConstants.STRING)
                        .append(DatabaseConnectorConstants.DOUBLE_QUOTE);
            } else if (dataTypes.get(param).equals(Types.INTEGER) || dataTypes.get(param).equals(Types.NUMERIC)
                    || dataTypes.get(param).equals(Types.BIGINT) || dataTypes.get(param).equals(Types.DECIMAL)
                    || dataTypes.get(param).equals(Types.DOUBLE) || dataTypes.get(param).equals(Types.FLOAT)
                    || dataTypes.get(param).equals(Types.REAL) || dataTypes.get(param).equals(Types.TINYINT)
                    || dataTypes.get(param).equals(Types.SMALLINT)) {
                sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE).append(
                        BACKSLASH_COLON).append(DatabaseConnectorConstants.INTEGER).append(
                        DatabaseConnectorConstants.DOUBLE_QUOTE);
            } else if (dataTypes.get(param).equals(Types.BOOLEAN) || dataTypes.get(param).equals(Types.BIT)) {
                sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE).append(
                        BACKSLASH_COLON).append(DatabaseConnectorConstants.BOOLEAN).append(
                        DatabaseConnectorConstants.DOUBLE_QUOTE);
            } else if (Objects.equals(dataTypes.get(param),Types.REF_CURSOR)) {
                sbSchema.append(DatabaseConnectorConstants.DOUBLE_QUOTE).append(JSONUtil.SCHEMA_TYPE).append(
                        BACKSLASH_COLON).append(DatabaseConnectorConstants.ARRAY).append(
                        DatabaseConnectorConstants.DOUBLE_QUOTE);
            }

            sbSchema.append("},");
        }

        return closeAndStringifyJsonSchema(false, sbSchema, jsonSchema);
    }

    /**
     * This method will get the Json Schema for Dynamic Update, Stored procedure and
     * Dynamic Delete Response.
     *
     * @param opsType the ops type
     * @return json
     */
    public static String getQueryJsonSchema(String opsType) {

        ObjectMapper mapper = DBv2JsonUtil.getObjectMapper();
        String json = null;
        try {
            SchemaFactoryWrapper wrapper = new SchemaFactoryWrapper();

            if (OperationTypeConstants.DYNAMIC_UPDATE.equals(opsType)) {
                mapper.acceptJsonFormatVisitor(UpdatePojo.class, wrapper);
            } else if (OperationTypeConstants.DYNAMIC_DELETE.equals(opsType)) {
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
     * Get the procedure name from the Object ID
     *
     * @param objectTypeId
     * @return the procedure name
     */
    public static String getProcedureName(String objectTypeId) {
        String procedure = null;
        if (objectTypeId != null && objectTypeId.lastIndexOf(".") > 0) {
            procedure = objectTypeId.substring(objectTypeId.lastIndexOf(".") + 1);
        } else {
            procedure = objectTypeId;
        }
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
        } else {
            procedurePackage = null;
        }
        return procedurePackage;
    }
}
