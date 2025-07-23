// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.util;

import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.MYSQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.ORACLE;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.POSTGRESQL;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TYPE_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.UNKNOWN_DATATYPE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaBuilderUtilTest {

    private static final String OBJECT_TYPE_ID_EVENT = "EVENT";
    private static final String OBJECT_TYPE_ID_CUSTOMER = "CUSTOMER";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_NAME_ID = "id";
    private static final String SCHEMA_NAME = "Schema Name";
    private static final  String SCHEMA_NOT_MATCHING_EXCEPTION = "Actual Schema is not matching with expected";
    private static final  String SCHEMA_PREFIX = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\""+",";
    private static final  String SCHEMA_PREFIX_TYPE_OBJECT = "\"type\":\"object\",";
    private static final  String SCHEMA_PREFIX_TYPE_ARRAY = "\"type\":\"array\",";
    private static final  String DATA_TYPE_STRING = "12";
    private static final  String DATA_TYPE_INTEGER = "4";

    private final Connection _connection = mock(Connection.class);
    private final ResultSet _resultSetEventWithSchema = mock(ResultSet.class);
    private final ResultSet _resultSetEventWithoutSchema = mock(ResultSet.class);
    private final ResultSet _resultSetCustomerWithSchema = mock(ResultSet.class);
    private final ResultSet _resultSetCustomerWithoutSchema = mock(ResultSet.class);
    private final ResultSet _resultSetForOracleMetadata = mock(ResultSet.class);
    private final ResultSet _resultSetForPostGreMetadata = mock(ResultSet.class);
    private final DatabaseMetaData _databaseMetaData = mock(DatabaseMetaData.class);

    @Before
    public void setup() throws SQLException {
        when(_connection.getMetaData()).thenReturn(_databaseMetaData);
    }

    private List<String> getInParams() {
        return Arrays.asList(COLUMN_NAME_NAME);
    }

    private void setResultSetEventWithSchemaName() throws SQLException {

        when(_databaseMetaData.getColumns(null, SCHEMA_NAME, OBJECT_TYPE_ID_EVENT, null)).thenReturn(
                _resultSetEventWithSchema);

        when(_resultSetEventWithSchema.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(DATA_TYPE_STRING);
        when(_resultSetEventWithSchema.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(_resultSetEventWithSchema.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        when(_resultSetEventWithSchema.next()).thenReturn(true).thenReturn(false);
    }

    private void setResultSetCustomerWithSchemaName() throws SQLException {

        when(_databaseMetaData.getColumns(null, SCHEMA_NAME, OBJECT_TYPE_ID_CUSTOMER, null)).thenReturn(
                _resultSetCustomerWithSchema);

        when(_resultSetCustomerWithSchema.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(
                DATA_TYPE_INTEGER);
        when(_resultSetCustomerWithSchema.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.INTEGER);
        when(_resultSetCustomerWithSchema.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_ID);
        when(_resultSetCustomerWithSchema.next()).thenReturn(true).thenReturn(false);
    }

    private void setResultSetEventWithOutSchemaName() throws SQLException {

        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_EVENT, null)).thenReturn(
                _resultSetEventWithoutSchema);

        when(_resultSetEventWithoutSchema.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(DATA_TYPE_STRING);
        when(_resultSetEventWithoutSchema.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(_resultSetEventWithoutSchema.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        when(_resultSetEventWithoutSchema.next()).thenReturn(true).thenReturn(false);
    }

    private void setResultSetCustomerWithoutSchemaName() throws SQLException {

        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_CUSTOMER, null)).thenReturn(
                _resultSetCustomerWithoutSchema);

        when(_resultSetCustomerWithoutSchema.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(
                DATA_TYPE_INTEGER);
        when(_resultSetCustomerWithoutSchema.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.INTEGER);
        when(_resultSetCustomerWithoutSchema.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_ID);
        when(_resultSetCustomerWithoutSchema.next()).thenReturn(true).thenReturn(false);
    }

    private void setProcedureMetadataForPostGreSQLDatabase() throws SQLException {

        when(_connection.getCatalog()).thenReturn(PROCEDURE_PACKAGE_NAME);
        when(_databaseMetaData.getProcedureColumns(PROCEDURE_PACKAGE_NAME, SCHEMA_NAME, OBJECT_TYPE_ID_EVENT,
                null)).thenReturn(_resultSetForPostGreMetadata);

        when(_resultSetForPostGreMetadata.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        when(_resultSetForPostGreMetadata.getString(6)).thenReturn("6");
        when(_resultSetForPostGreMetadata.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(_resultSetForPostGreMetadata.getString(PROCEDURE_PACKAGE_NAME)).thenReturn(PROCEDURE_PACKAGE_NAME);
        when(_resultSetForPostGreMetadata.next()).thenReturn(true).thenReturn(false);
    }

    private void setProcedureMetadataForOracleDatabase() throws SQLException {

        when(_databaseMetaData.getProcedureColumns(PROCEDURE_PACKAGE_NAME, SCHEMA_NAME, OBJECT_TYPE_ID_EVENT,
                null)).thenReturn(_resultSetForOracleMetadata);

        when(_resultSetForOracleMetadata.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn(
                DATA_TYPE_STRING);
        when(_resultSetForOracleMetadata.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        when(_resultSetForOracleMetadata.getString(6)).thenReturn(UNKNOWN_DATATYPE);
        when(_resultSetForOracleMetadata.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(_resultSetForOracleMetadata.getString(PROCEDURE_PACKAGE_NAME)).thenReturn(PROCEDURE_PACKAGE_NAME);
        when(_resultSetForOracleMetadata.next()).thenReturn(true).thenReturn(false);
    }

    @Test
    public void getJsonSchemaTestOracleBatchingSingle() throws SQLException {

        String expectedSchema =
                SCHEMA_PREFIX + "\"type\":\"array\",\"items\":{"
                        + "\"type\":\"object\",\"properties\":{\"SQLQuery\":{\"type\":\"string\"}}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(ORACLE);
        setResultSetEventWithSchemaName();

        String actualSchema = SchemaBuilderUtil.getJsonSchema(_connection, OBJECT_TYPE_ID_EVENT, true, true, true,
                SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getJsonSchemaTestMySqlNonBatchingSingle() throws SQLException {

        String expectedSchema =  SCHEMA_PREFIX +
                SCHEMA_PREFIX_TYPE_OBJECT + "\"properties\":{\"SQLQuery\":{\"type\":\"string\"}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(MYSQL);
        setResultSetCustomerWithSchemaName();

        String actualSchema = SchemaBuilderUtil.getJsonSchema(_connection, OBJECT_TYPE_ID_CUSTOMER, true, true, false,
                SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getJsonSchemaTestMySqlBatchingMultiple() throws SQLException {

        String expectedSchema =  SCHEMA_PREFIX + SCHEMA_PREFIX_TYPE_ARRAY
                + "\"items\":{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\"},"
                + "\"SQLQuery\":{\"type\":\"string\"}," + "\"id\":{\"type\":\"integer\"}}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(MYSQL);

        setResultSetCustomerWithSchemaName();
        setResultSetCustomerWithoutSchemaName();
        setResultSetEventWithSchemaName();
        setResultSetEventWithOutSchemaName();

        String actualSchema = SchemaBuilderUtil.getJsonSchema(_connection,
                OBJECT_TYPE_ID_EVENT + "," + OBJECT_TYPE_ID_CUSTOMER, true, true, true, SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getJsonSchemaTestMySqlWithoutOutput() throws SQLException {

        String expectedSchema =  SCHEMA_PREFIX + SCHEMA_PREFIX_TYPE_ARRAY
                + "\"items\":{\"type\":\"object\",\"properties\":{\"EVENT.name\":{\"type\":\"string\"},"
                + "\"SQLQuery\":{\"type\":\"string\"},\"CUSTOMER.id\":{\"type\":\"integer\"}}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(MYSQL);
        setResultSetCustomerWithSchemaName();
        setResultSetCustomerWithoutSchemaName();
        setResultSetEventWithSchemaName();
        setResultSetEventWithOutSchemaName();

        String actualSchema = SchemaBuilderUtil.getJsonSchema(_connection,
                OBJECT_TYPE_ID_EVENT + "," + OBJECT_TYPE_ID_CUSTOMER, true, false, true, SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getProcedureSchemaOracleTest() throws SQLException {

        String expectedSchema =  SCHEMA_PREFIX + SCHEMA_PREFIX_TYPE_OBJECT
                + "\"properties\":{\"name\":{\"type\":\"string\"}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(ORACLE);
        setProcedureMetadataForOracleDatabase();

        String actualSchema = SchemaBuilderUtil.getProcedureSchema(_connection,
                PROCEDURE_PACKAGE_NAME + "." + OBJECT_TYPE_ID_EVENT, getInParams(), SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getProcedureSchemaPostGreSqlTest() throws SQLException {

        String expectedSchema = SCHEMA_PREFIX + SCHEMA_PREFIX_TYPE_OBJECT
                + "\"properties\":{\"name\":{\"type\":\"integer\"}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(POSTGRESQL);
        setProcedureMetadataForPostGreSQLDatabase();

        String actualSchema = SchemaBuilderUtil.getProcedureSchema(_connection,
                PROCEDURE_PACKAGE_NAME + "." + OBJECT_TYPE_ID_EVENT, getInParams(), SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getQueryJsonSchemaDynamicUpdateTest() {

        String expectedSchema =
                "{\"type\":\"object\",\"properties\":{\"SET\":{\"type\":\"array\",\"items\":{\"type\":\"object\","
                        + "\"properties\":{\"column\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"}}}},"
                        + "\"WHERE\":{\"type\":\"array\",\"items\":{\"type\":\"object\","
                        + "\"properties\":{\"column\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"},"
                        + "\"operator\":{\"type\":\"string\"}}}}}}";

        String actualSchema = SchemaBuilderUtil.getQueryJsonSchema(OperationTypeConstants.DYNAMIC_UPDATE);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getQueryJsonSchemaDynamicDeleteTest() {

        String expectedSchema =
                "{\"type\":\"object\",\"properties\":{\"WHERE\":{\"type\":\"array\",\"items\":{\"type\":\"object\","
                        + "\"properties\":{\"column\":{\"type\":\"string\"},\"value\":{\"type\":\"string\"},"
                        + "\"operator\":{\"type\":\"string\"}}}}}}";

        String actualSchema = SchemaBuilderUtil.getQueryJsonSchema(OperationTypeConstants.DYNAMIC_DELETE);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    @Test
    public void getQueryJsonSchemaTest() {

        String expectedSchema = "{\"type\":\"object\",\"properties\":{\"Status\":{\"type\":\"string\"},"
                + "\"Query\":{\"type\":\"string\"},\"Rows Effected\":{\"type\":\"number\"," + "\"type\":\"integer\"}}}";

        String actualSchema = SchemaBuilderUtil.getQueryJsonSchema("");

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }

    /**
     * Tests schema generation for an Oracle stored procedure returning a REF CURSOR.
     * Verifies the generated schema matches the expected format.
     */
    @Test
    public void getProcedureSchemaOracleTestForRefCursor() throws SQLException {

        String expectedSchema =  SCHEMA_PREFIX + SCHEMA_PREFIX_TYPE_OBJECT
                + "\"properties\":{\"name\":{\"type\":\"array\"}}}";

        when(_databaseMetaData.getDatabaseProductName()).thenReturn(ORACLE);
        setProcedureMetadataForOracleDatabase();
        when(_resultSetForOracleMetadata.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        when(_resultSetForOracleMetadata.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.REF_CURSOR);

        String actualSchema = SchemaBuilderUtil.getProcedureSchema(_connection,
                PROCEDURE_PACKAGE_NAME + "." + OBJECT_TYPE_ID_EVENT, getInParams(), SCHEMA_NAME);

        assertEquals(SCHEMA_NOT_MATCHING_EXCEPTION, expectedSchema, actualSchema);
    }
}
