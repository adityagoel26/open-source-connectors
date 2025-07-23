// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.operations;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationType;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeBrowser;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;

import com.boomi.snowflake.util.SnowflakeOperationUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.jdbc.internal.net.minidev.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.mockito.Mockito.mock;

@RunWith(PowerMockRunner.class)
@PrepareForTest(SnowflakeOperationUtil.class)
public class SnowFlakeBrowserTest extends BaseTestOperation {

    private static final String OBJECTDATA = "BULKOPERATIONS_ALLDATATYPE";
    private static final int COL_COUNT = 2;
    private static final String RESULT = "{\"type\":\"string\"}";
    private static final String APPLE = "apple";
    private SnowflakeBrowser _snowflakeBrowser;
    private Connection _connection;
    private String _objectTypeId = "NA";
    private DatabaseMetaData _databaseMetaData;
    private ResultSet _resultSet;
    private PreparedStatement _preparedStatement;
    private ResultSetMetaData _resultSetMetaData;
    private BrowseContext _context;
    DatabaseMetaData databaseMetaData;
    ResultSet resultSet, columnsRS, columnsRS2;
    Statement statement1, statement2;
    private com.boomi.snowflake.SnowflakeConnection _snowflakeConnection;
    private ObjectMapper mockObjectMapper;


    @Before
    public void setup() throws SQLException {
        _snowflakeBrowser = Mockito.mock(SnowflakeBrowser.class);
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        _snowflakeConnection= Mockito.mock(SnowflakeConnection.class);
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        _connection = Mockito.mock(Connection.class);
        _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        _resultSet = Mockito.mock(ResultSet.class);
        mockObjectMapper = Mockito.mock(ObjectMapper.class);
        _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
        _preparedStatement = Mockito.mock(PreparedStatement.class);
        _context = Mockito.mock(BrowseContext.class);
        databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        resultSet = Mockito.mock(ResultSet.class);
        columnsRS = Mockito.mock(ResultSet.class);
        columnsRS2 = Mockito.mock(ResultSet.class);
        statement1 = Mockito.mock(Statement.class);
        statement2 = Mockito.mock(Statement.class);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.getCatalog()).thenReturn(OBJECTDATA);
        Mockito.when(_connection.getSchema()).thenReturn("PUBLIC");
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnName(ArgumentMatchers.anyInt())).thenReturn("cname");
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(COL_COUNT);
        Mockito.when(_resultSet.getString(ArgumentMatchers.anyInt())).thenReturn(APPLE);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSetMetaData.getColumnName(ArgumentMatchers.anyInt())).thenReturn(APPLE);
        Mockito.when(_resultSetMetaData.getColumnTypeName(ArgumentMatchers.anyInt())).thenReturn("String");
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    }

    @Test
    public void getObjectProperties_withMetaData() {
        String fname = "fname";
        String lname = "lname";
        SortedMap<String, String> metadata = new TreeMap<String, String>();
        metadata.put(fname, "mary");
        metadata.put(lname, "smith");
        JSONObject jsonObject = _snowflakeBrowser.getObjectProperties(_objectTypeId, _connection, metadata);
        Assert.assertTrue(!jsonObject.isEmpty());
        Assert.assertEquals(2, jsonObject.size());
        Assert.assertEquals(RESULT, jsonObject.get(fname).toString());
        Assert.assertEquals(RESULT, jsonObject.get(lname).toString());
    }

    @Test
    public void getObjectProperties_withOutMetaData_withResultSet() throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true,false);
        SortedMap<String, String> metadata = new TreeMap<>();
        JSONObject jsonObject = _snowflakeBrowser.getObjectProperties(_objectTypeId, _connection, metadata);
        Assert.assertTrue(!jsonObject.isEmpty());
        Assert.assertEquals(1, jsonObject.size());
        Assert.assertEquals(RESULT, jsonObject.get(APPLE).toString());
    }

    @Test
    public void getObjectProperties_withOutMetaData_withOutResultSet() throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(false);
        SortedMap<String, String> metadata = new TreeMap<String, String>();
        JSONObject jsonObject = _snowflakeBrowser.getObjectProperties(_objectTypeId, _connection, metadata);
        Assert.assertTrue(!jsonObject.isEmpty());
        Assert.assertEquals(1, jsonObject.size());
        Assert.assertEquals(RESULT, jsonObject.get(APPLE).toString());
    }

    /**
     * Tests `displayConnectionFields` with overrides enabled.
     * Verifies that the `ObjectDefinitions` fields are correctly set with overridden values.
     */
    @Test
    public void testDisplayConnectionFields() {
        PropertyMap operationProperties = createPropertyMap("override db", "override schema", true);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).displayConnectionFields(
                ArgumentMatchers.any(PropertyMap.class), ArgumentMatchers.any(ObjectDefinitions.class));
        ObjectDefinitions definitions = new ObjectDefinitions();
        _snowflakeBrowser.displayConnectionFields(operationProperties, definitions);
        Assert.assertEquals(Boolean.TRUE,
                operationProperties.getBooleanProperty(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE));
        Assert.assertEquals(2, definitions.getOperationFields().size());
        Assert.assertEquals("override db", definitions.getOperationFields().get(0).getDefaultValue());
        Assert.assertEquals("override schema", definitions.getOperationFields().get(1).getDefaultValue());
        Assert.assertEquals("Database Name", definitions.getOperationFields().get(0).getLabel());
        Assert.assertEquals("Schema Name", definitions.getOperationFields().get(1).getLabel());
        Assert.assertEquals("db", definitions.getOperationFields().get(0).getId());
        Assert.assertEquals("schema", definitions.getOperationFields().get(1).getId());
        Assert.assertEquals("Database name, Case-sensitive if written between double quotation marks.",
                definitions.getOperationFields().get(0).getHelpText());
        Assert.assertEquals("Schema name, Case-sensitive if written between double quotation marks.",
                definitions.getOperationFields().get(1).getHelpText());
        Assert.assertEquals("string", definitions.getOperationFields().get(0).getType().value());
        Assert.assertEquals("string", definitions.getOperationFields().get(1).getType().value());
        Assert.assertEquals(Boolean.TRUE, definitions.getOperationFields().get(0).isOverrideable());
        Assert.assertEquals(Boolean.TRUE, definitions.getOperationFields().get(1).isOverrideable());
    }

    /**
     * Tests `displayConnectionFields` when connection settings override is enabled.
     * Verifies that no operation fields are added to `ObjectDefinitions`.
     */
    @Test
    public void testDisplayConnectionFieldsWithOutEnable() {
        PropertyMap operationProperties = createPropertyMap("", "", false);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).displayConnectionFields(
                ArgumentMatchers.any(PropertyMap.class), ArgumentMatchers.any(ObjectDefinitions.class));
        ObjectDefinitions definitions = new ObjectDefinitions();
        _snowflakeBrowser.displayConnectionFields(operationProperties, definitions);
        Assert.assertEquals(Boolean.FALSE,
                operationProperties.getBooleanProperty(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE));
        Assert.assertEquals(0, definitions.getOperationFields().size());
    }

    /**
     * Tests {@code displayConnectionFields} with empty values in properties and override enabled.
     * Verifies that two operation fields are added and the {@code ENABLECONNECTIONOVERRIDE} property is {@code true}.
     */
    @Test
    public void testDisplayConnectionFieldsWithEmptyValues() {
        PropertyMap operationProperties = createPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING, true);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).displayConnectionFields(
                ArgumentMatchers.any(PropertyMap.class), ArgumentMatchers.any(ObjectDefinitions.class));
        ObjectDefinitions definitions = new ObjectDefinitions();
        _snowflakeBrowser.displayConnectionFields(operationProperties, definitions);
        Assert.assertEquals(Boolean.TRUE,
                operationProperties.getBooleanProperty(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE));
        Assert.assertEquals(2, definitions.getOperationFields().size());
        Assert.assertEquals(Boolean.TRUE, definitions.getOperationFields().get(0).isOverrideable());
        Assert.assertEquals(Boolean.TRUE, definitions.getOperationFields().get(1).isOverrideable());
        Assert.assertEquals(StringUtil.EMPTY_STRING, definitions.getOperationFields().get(0).getDefaultValue());
        Assert.assertEquals(StringUtil.EMPTY_STRING, definitions.getOperationFields().get(1).getDefaultValue());
    }

    /**
     * Tests `overrideSchemaValue` when schema override is enabled.
     * Verifies that the schema value is overridden correctly.
     */
    @Test
    public void testOverrideSchemaValue() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "new schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        String result = Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Assert.assertEquals("NEW SCHEMA", result);
    }

    /**
     * Tests `overrideSchemaValue` when schema override is disabled.
     * Verifies that the original schema value is returned.
     */
    @Test
    public void testOverrideSchemaValueWithOutEnable() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "new schema", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        String result = Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Assert.assertEquals("NEW SCHEMA", result);
    }

    /**
     * Tests the retrieval of object types from the Snowflake browser.
     * Validates the behavior when the context returns a custom operation type of "GET".
     */
    @Test
    public void testGetObjectTypesForBulkGet() {
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getCustomOperationType()).thenReturn("GET");
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectTypes();
        ObjectTypes objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertEquals("Dummy_Profile", objectTypes.getTypes().get(0).getLabel());
    }

    /**
     * Creates a `PropertyMap` with specified database, schema values, and override flag.
     * Sets the provided values for database, schema, and connection override.
     */
    private static PropertyMap createPropertyMap(String dbValue, String schemaValue, boolean override) {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put(SnowflakeOverrideConstants.DATABASE, dbValue);
        propertyMap.put(SnowflakeOverrideConstants.SCHEMA, schemaValue);
        propertyMap.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE, override);
        return propertyMap;
    }

    @Test
    public void testGetObjectTypes() {
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getCustomOperationType()).thenReturn("PUT");
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectTypes();
        ObjectTypes objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        ObjectType objectType = objectTypes.getTypes().get(0);
        Assert.assertEquals("f9d57e4a-7740-479d-8110-b3f0ab2bd3dc", objectType.getId());
        Assert.assertEquals("Dummy_Profile", objectType.getLabel());
    }

    /**
     * Tests the getModifiedFillSPObjectType method to ensure that
     * the schema name has escaped quotes removed correctly.
     */
    @Test
    public void testGetModifiedFillSPObjectType() throws Exception {
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getCustomOperationType()).thenReturn("EXECUTE");
        String db = "DATABASE";
        String schema = "\"schema\"";
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from "
                + "\"DATABASE\".\"INFORMATION_SCHEMA\".\"PROCEDURES\" where procedure_schema = 'schema'";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWithBlank() throws Exception {
        PropertyMap operationProperties = createPropertyMap(" ", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        //Mockito.doCallRealMethod().when(_snowflakeBrowser).overrideCatalogDetails(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbccatalog");
        String actual = Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Assert.assertEquals(" ", actual);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWitDBEmpty() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbccatalog");
        String actual = Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Assert.assertEquals(null, actual);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWithDBNull() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbccatalog");
        String actual = Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Assert.assertEquals(null, actual);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWithDValid() throws Exception {
        PropertyMap operationProperties = createPropertyMap("Test_DB", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbccatalog");
        String returnField=Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Mockito.verify(_connection,Mockito.times(0)).getCatalog();
        Assert.assertNotNull(returnField);
        Assert.assertEquals("TEST_DB", returnField);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWithDBWithValidDoubleQuotes() throws Exception {
        PropertyMap operationProperties = createPropertyMap("\"Test_DB\"", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbccatalog");
        String returnField=Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Mockito.verify(_connection,Mockito.times(0)).getCatalog();
        Assert.assertNotNull(returnField);
        Assert.assertEquals("Test_DB", returnField);


    }

    /**
     * Test the behavior of `getStoredProcedures` when the database and schema are empty and overridden.
     * Verifies that the returned object has zero stored procedures.
     */
    @Test
    public void testGetStoredProceduresForEmptyDBAndSchemaWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "TEST_SCHEMA", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database and schema are empty without override.
     * Verifies that the returned object has zero stored procedures when no catalog is provided.
     */
    @Test
    public void testGetStoredProceduresForEmptyDBAndSchemaWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "", false);
        PropertyMap connectionProperties = createPropertyMap("", "", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn(null);
        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is blank and the schema is empty with override.
     * Verifies that the returned object has zero stored procedures with the provided database and schema.
     */
    @Test
    public void testGetStoredProceduresForDBBlankAndEmptySchemaWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap(" ", "", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "TEST_SCHEMA", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is provided but the schema is empty without override.
     * Verifies that the returned object has zero stored procedures when the catalog is blank.
     */
    @Test
    public void testGetStoredProceduresForDBBlankAndEmptySchemaWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        PropertyMap connectionProperties = createPropertyMap(" ", "", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn(" ");


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when both the database and schema are blank with override.
     * Verifies that the returned object has zero stored procedures with the provided database and schema.
     */
    @Test
    public void testGetStoredProceduresForDBBlankAndSchemaBlankWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap(" ", " ", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "TEST_SCHEMA", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when both the database and schema are blank without override.
     * Verifies that the returned object has zero stored procedures when the catalog is blank.
     */
    @Test
    public void testGetStoredProceduresForDBBlankAndSchemaBlankWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        PropertyMap connectionProperties = createPropertyMap(" ", " ", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn(" ");


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is empty and the schema is blank without override.
     * Verifies that the returned object has zero stored procedures when the catalog is null.
     */
    @Test
    public void testGetStoredProceduresForDBEmptyAndSchemaBlankWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap(null, null, false);
        PropertyMap connectionProperties = createPropertyMap("", " ", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn(null);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is empty and the schema is blank with override.
     * Verifies that the returned object has zero stored procedures with the provided database and schema.
     */
    @Test
    public void testGetStoredProceduresForDBEmptyAndSchemaBlankWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", " ", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is empty and the schema is valid without override.
     * Verifies that the returned object has one stored procedure when the catalog is null but a valid schema is provided.
     */
    @Test
    public void testGetStoredProceduresForEmptyDBAndValidSchemaWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        PropertyMap connectionProperties = createPropertyMap("", "PUBLIC", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn(null);
        Mockito.when(_connection.getMetaData().getCatalogs()).thenReturn(_resultSet);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(1L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is empty and the schema is valid with override.
     * Verifies that the returned object has one stored procedure when the catalog is provided with override enabled.
     */
    @Test
    public void testGetStoredProceduresForEmptyDBAndValidSchemaWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "PUBLIC", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "PUBLIC", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData().getCatalogs()).thenReturn(_resultSet);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(1L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is valid and schema is empty without override.
     * Verifies that the returned object has zero stored procedures when the catalog is valid and no procedures are found.
     */
    @Test
    public void testGetStoredProceduresForValidDBAndEmptySchemaWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn("TEST_DB");
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.any())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn( false);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is valid and the schema is empty with override.
     * Verifies that the returned object has zero stored procedures when no procedures are found in the valid database.
     */
    @Test
    public void testGetStoredProceduresForValidDBAndEmptySchemaWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "PUBLIC", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.any())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn( false);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is valid and the schema is blank without override.
     * Verifies that the returned object has zero stored procedures when the catalog is valid and no procedures are found.
     */
    @Test
    public void testGetStoredProceduresForValidDBAndBlankSchemaWithoutOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "PUBLIC", false);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", " ", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn("TEST_DB");
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.any())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn( false);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database is valid and the schema is blank with override.
     * Verifies that the returned object has zero stored procedures when no procedures are found and override is enabled.
     */
    @Test
    public void testGetStoredProceduresForValidDBAndBlankSchemaWithOverride() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", " ", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "PUBLIC", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);


        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.any())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn( false);


        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Test the behavior of `getStoredProcedures` when the database name contains valid double quotes with override.
     * Verifies that the `getCatalog` method is not called when the database is overridden with double-quoted name.
     */
    @Test
    public void testOverrideCatalogDetailsWithDBWithValidDoubleQuotesForStoredProcedures() throws Exception {
        PropertyMap operationProperties = createPropertyMap("\"Test_DB\"", "newSchema", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "newSchema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        ObjectTypes returnField=Whitebox.invokeMethod(_snowflakeBrowser,"getStoredProcedures");
        Mockito.verify(_connection,Mockito.times(0)).getCatalog();
        Assert.assertNotNull(returnField);
    }

    /**
     * Tests `OverrideCatalogDetails` when db override is enabled.
     * Verifies that the db value is overridden correctly.
     */
    @Test
    public void testOverrideCatalogDetailsWithOutOverrideEnable() throws Exception {
        PropertyMap operationProperties = createPropertyMap("TEST_DB", "newSchema", false);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_connection.getCatalog()).thenReturn("jdbcCatalog");
        String actual = Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",_connection);
        Assert.assertEquals("jdbcCatalog",actual);
    }

    /**
     * Tests `getStreamfilter` with valid catalog and valid schema
     * Verifies filter
     */
    @Test
    public void testGetStreamFilterWithCatalogAndSchema() throws Exception {
        // Test when only catalog is provided
        String catalog = "TestCatalog";
        String inputSchema = "TestSchema";
        String expected = " in schema \"TestCatalog\".TestSchema";
        String result = Whitebox.invokeMethod(_snowflakeBrowser, "getStreamFilter", inputSchema, catalog);
        Assert.assertEquals(expected, result);
    }

    /**
     * Tests `getStreamfilter` with only catalog
     * Verifies filter
     */
    @Test
    public void testGetStreamFilterWithOnlyCatalog() throws Exception {
        // Test when only catalog is provided
        String catalog = "TestCatalog";
        String inputSchema = null;
        String expected = " in database \"TestCatalog\"";
        String result = Whitebox.invokeMethod(_snowflakeBrowser, "getStreamFilter", inputSchema, catalog);
        Assert.assertEquals(expected, result);
    }

    /**
     * Tests `getStreamfilter` with only schema
     * Verifies filter
     */
    @Test
    public void testGetStreamFilterWithOnlySchema() throws Exception {
        // Test when only inputSchema is provided
        String catalog = null;
        String inputSchema = "TestSchema";
        String expected = " in schema TestSchema";
        String result = Whitebox.invokeMethod(_snowflakeBrowser, "getStreamFilter", inputSchema, catalog);
        Assert.assertEquals(expected, result);
    }

    /**
     * Tests `getStreamfilter` with null schema and null catalog
     * Verifies filter
     */
    @Test
    public void testGetStreamFilterWithNullSchemaAndCatalog() throws Exception {
        // Test when both catalog and inputSchema are null
        String catalog = null;
        String inputSchema = null;
        String expected = "";
        String result = Whitebox.invokeMethod(_snowflakeBrowser, "getStreamFilter", inputSchema, catalog);
        Assert.assertEquals(expected, result);
    }

    /**
     * Tests `getObjects`.
     * Verifies that the db and schema value is overridden correctly.
     */
    @Test
    public void testgetObjects() throws Exception {
        PropertyMap operationProperties = createPropertyMap("CON_Db", "CON_Schema", true);
        PropertyMap connectionProperties = createPropertyMap("Test_DB", "Test_Schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Connection connectionMock = Mockito.mock(Connection.class);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);

        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connectionMock);
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Mockito.when(connectionMock.getCatalog()).thenReturn("CON_DB");
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",connectionMock);
        Mockito.when(connectionMock.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getString(4)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(3)).thenReturn("EMPTABLE");
        Mockito.when(_resultSet.getString(2)).thenReturn("CON_Schema");
        Mockito.when(_resultSet.getString(1)).thenReturn("CON_DB");
        ObjectTypes objectTypes = Whitebox.invokeMethod(_snowflakeBrowser, "getObjects",new String[] { "TABLE" }, false);
        Assert.assertNotNull(objectTypes);
        ObjectType objectType = objectTypes.getTypes().get(0);
        Assert.assertEquals("\"EMPTABLE\"", objectType.getId());
        Assert.assertEquals("CON_DB.CON_Schema.EMPTABLE\t(TABLE)", objectType.getLabel());
    }

    /**
     * Tests `getObjects` with stream true.
     * Verifies that the db and schema value is overridden correctly.
     */
    @Test
    public void testgetObjectsWithStream() throws Exception {
        PropertyMap operationProperties = createPropertyMap("CON_Db", "CON_Schema", true);
        PropertyMap connectionProperties = createPropertyMap("Test_DB", "Test_Schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Connection connectionMock = Mockito.mock(Connection.class);
        PreparedStatement preparedStatementMock = Mockito.mock(PreparedStatement.class);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);

        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connectionMock);
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Mockito.when(connectionMock.getCatalog()).thenReturn("CON_DB");
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",connectionMock);
        Mockito.when(connectionMock.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getString(4)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(3)).thenReturn("EMPTABLE");
        Mockito.when(_resultSet.getString(2)).thenReturn("CON_Schema");
        Mockito.when(_resultSet.getString(1)).thenReturn("CON_DB");

        Mockito.when(connectionMock.prepareStatement(ArgumentMatchers.anyString())).thenReturn(preparedStatementMock);
        ResultSet resultSetWithStatement = Mockito.mock(ResultSet.class);
        Mockito.when(preparedStatementMock.executeQuery()).thenReturn(resultSetWithStatement);
        Mockito.when(preparedStatementMock.executeQuery().next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetWithStatement.next()).thenReturn(true).thenReturn(false);
        Mockito.when(resultSetWithStatement.getString(4)).thenReturn("TABLE");
        Mockito.when(resultSetWithStatement.getString(3)).thenReturn("STREAMTABLE");
        Mockito.when(resultSetWithStatement.getString(2)).thenReturn("CON_Schema");
        Mockito.when(resultSetWithStatement.getString(1)).thenReturn("CON_DB");
        ObjectTypes objectTypes = Whitebox.invokeMethod(_snowflakeBrowser, "getObjects",new String[] { "TABLE" }, true);
        Assert.assertNotNull(objectTypes);
        ObjectType objectType = objectTypes.getTypes().get(1);
        Assert.assertEquals("CON_Schema", objectType.getId());
        Assert.assertEquals("STREAMTABLE.TABLE.CON_Schema\t(STREAM)", objectType.getLabel());
    }

    /**
     * Tests `getObjects`.
     * Verifies that the db and schema value is overridden correctly.
     */
    @Test
    public void testgetObjectsForDbBlank() throws Exception {
        PropertyMap operationProperties = createPropertyMap(" ", "CON_Schema", true);
        PropertyMap connectionProperties = createPropertyMap("Test_DB", "Test_Schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Connection connectionMock = Mockito.mock(Connection.class);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);

        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connectionMock);
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Mockito.when(connectionMock.getCatalog()).thenReturn("CON_DB");
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",connectionMock);

        ObjectTypes objectTypes = Whitebox.invokeMethod(_snowflakeBrowser, "getObjects",new String[] { "TABLE" }, false);
        Assert.assertNotNull(objectTypes);
        List<ObjectType> listOfObject = objectTypes.getTypes();
        Assert.assertTrue(CollectionUtil.isEmpty(listOfObject));
    }

    /**
     * Tests `getObjects`.
     * Verifies that the db and schema value is overridden correctly.
     */
    @Test
    public void testgetObjectsForDbException() throws Exception {
        PropertyMap operationProperties = createPropertyMap(" ", "CON_Schema", true);
        PropertyMap connectionProperties = createPropertyMap("Test_DB", "Test_Schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Connection connectionMock = Mockito.mock(Connection.class);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);

        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connectionMock);
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideSchemaValue",
                _context.getOperationProperties().get("schema"));
        Mockito.doThrow(new SQLException()).when(connectionMock).getCatalog();
        Whitebox.invokeMethod(_snowflakeBrowser,"overrideCatalogDetails",connectionMock);
        ObjectTypes objectTypes = Whitebox.invokeMethod(_snowflakeBrowser, "getObjects",new String[] { "TABLE" }, false);
        Assert.assertNotNull(objectTypes);
        List<ObjectType> listOfObject = objectTypes.getTypes();
        Assert.assertTrue(CollectionUtil.isEmpty(listOfObject));
    }

    /**
     * Tests the getModifiedFillSPObjectType method to ensure that
     * the schema name is handled correctly when it does not include double quotes.
     * Validates that the generated SQL query matches the expected format.
     *
     * @throws Exception if any error occurs during method invocation
     */
    @Test
    public void testGetModifiedFillSPObjectTypeWithoutDoubleQuote() throws Exception {
        String db = "DATABASE";
        String schema = "schema";
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from "
                + "\"DATABASE\".\"INFORMATION_SCHEMA\".\"PROCEDURES\" where procedure_schema = 'schema'";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);
    }

    /**
     * Tests the getModifiedFillSPObjectType method to ensure that
     * it handles a null schema name correctly. Validates that the
     * generated SQL query excludes the schema filter when schema is null.
     *
     * @throws Exception if any error occurs during method invocation
     */
    @Test
    public void testGetModifiedFillSPObjectTypeWithSchemaNull() throws Exception {
        String db = "DATABASE";
        String schema = null;
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from "
                + "\"DATABASE\".\"INFORMATION_SCHEMA\".\"PROCEDURES\" ";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);
    }

    /**
     * Verifies that the private method {@code getModifiedFillSPObjectType} generates the correct SQL query
     * when the database parameter is {@code null} and a valid schema is provided.
     * Expected SQL query filters procedures based only on the schema.
     * @throws Exception if reflection or query generation fails.
     */
    @Test
    public void testGetModifiedFillSPObjectTypeWithDataBaseNull() throws Exception {
        String db = null;
        String schema = "schema";
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from " +
                "\"INFORMATION_SCHEMA\".\"PROCEDURES\" where procedure_schema = 'schema'";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);
    }

    /**
     * Tests the {@code getModifiedFillSPObjectType} method when both database and schema are null.
     * Verifies that the generated query excludes schema and database filters.
     *
     * @throws Exception if an error occurs during method invocation
     */
    @Test
    public void testGetModifiedFillSPObjectTypeWithDataBaseNullAndSchemaNull() throws Exception {
        String db = null;
        String schema = null;
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from " +
                "\"INFORMATION_SCHEMA\".\"PROCEDURES\" ";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);
    }

    /**
     * Tests the {@code getModifiedFillSPObjectType} method when both database and schema are empty.
     * Verifies that the generated query includes the empty database and schema in the expected format.
     *
     * @throws Exception if an error occurs during method invocation
     */
    @Test
    public void testGetModifiedFillSPObjectTypeWithDataBaseEmptyAndSchemaEmpty() throws Exception {
        String db = "";
        String schema = "";
        String expected = "select PROCEDURE_CATALOG,PROCEDURE_SCHEMA,PROCEDURE_NAME,ARGUMENT_SIGNATURE,DATA_TYPE from " +
                "\"\".\"INFORMATION_SCHEMA\".\"PROCEDURES\" where procedure_schema = ''";
        String query = Whitebox.invokeMethod(_snowflakeBrowser, "getModifiedFillSPObjectType", db, schema);
        Assert.assertEquals(expected, query);


    }

    /**
     * Tests the {@code getObjectTypes} method of the {@code SnowflakeBrowser} class.
     * <p>
     * Verifies that the method retrieves and constructs {@link ObjectTypes} and {@link ObjectType}
     * correctly using mocked dependencies and properties.
     * </p>
     *
     * @throws SQLException if an error occurs during the mocked SQL operations.
     */
    @Test
    public void testExecuteObjectTypes() throws SQLException {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("schema", "\"schema\"");
        PropertyMap operationProperties = createPropertyMap("", "new schema", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getCustomOperationType()).thenReturn("EXECUTE");
        Mockito.when(_context.getConnectionProperties()).thenReturn(propertyMap);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(_resultSet.getString(ArgumentMatchers.anyInt())).thenReturn("TEST");
        Mockito.when(_connection.getMetaData().getCatalogs()).thenReturn(_resultSet);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);

        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectTypes();
        ObjectTypes objectTypes = _snowflakeBrowser.getObjectTypes();

        Assert.assertNotNull(objectTypes);
        ObjectType objectType = objectTypes.getTypes().get(0);
        Assert.assertEquals("\"TEST\".\"TEST\".\"TEST\".TEST.TEST", objectType.getId());
        Assert.assertEquals("TEST.TEST.TEST.TEST.TEST", objectType.getLabel());
    }

    /**
     * Test the behavior of `getObjectTypes` when the custom operation type is blank.
     * Verifies that the returned `ObjectTypes` has zero types when the operation type is empty.
     */
    @Test
    public void testExecuteObjectTypesWithBlank(){
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getCustomOperationType()).thenReturn("");

        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectTypes();
        ObjectTypes objectTypes = _snowflakeBrowser.getObjectTypes();

        Assert.assertNotNull(objectTypes);
        int objectType = objectTypes.getTypes().size();
        Assert.assertEquals(0, objectType);
    }

    /**
     * Test the connection setup and catalog/schema assignment for Snowflake browser.
     * Verifies correct catalog and schema values are applied to the connection.
     */
    @Test
    public void testRestConnection() throws Exception {
        PropertyMap connectionProperties = createPropertyMap("TEST_SF_DB", "SCHEMA", true);
        connectionProperties.put("enablePooling", true);
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE,true);

        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_snowflakeConnection.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);

        Whitebox.invokeMethod(_snowflakeBrowser, "closeAndResetConnectionIfRequired",
                _snowflakeConnection, _connection);

        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(_connection, Mockito.times(1)).getCatalog();
        Mockito.verify(_connection, Mockito.times(1)).getSchema();
        Mockito.verify(_connection).setCatalog(catalogCaptor.capture());
        Mockito.verify(_connection).setSchema(schemaCaptor.capture());

        Assert.assertEquals(connectionProperties.getProperty(SnowflakeOverrideConstants.DATABASE).toUpperCase(),
                catalogCaptor.getValue());
        Assert.assertEquals(connectionProperties.getProperty(SnowflakeOverrideConstants.SCHEMA).toUpperCase(),
                schemaCaptor.getValue());

        Mockito.verify(_connection, Mockito.times(1)).close();
    }

    /**
     * Test handling of SQLExceptions during connection setup in Snowflake browser.
     * Verifies that a ConnectorException is thrown when an SQLException occurs.
     */
    @Test(expected = ConnectorException.class)
    public void testRestConnectionWithSqlException() throws Exception {
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);

        PropertyMap connectionProperties = createPropertyMap("TEST_SF_DB", "SCHEMA", true);
        connectionProperties.put("enablePooling", true);
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE,true);

        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(_snowflakeConnection.getContext()).thenReturn(_context);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(_connection.getCatalog()).thenThrow(new SQLException());
        Whitebox.invokeMethod(_snowflakeBrowser, "closeAndResetConnectionIfRequired",
                _snowflakeConnection, _connection);
        Mockito.verify(_connection, Mockito.times(1)).close();
    }

    /**
     * Test behavior when the connection is null during the connection setup.
     * Verifies that no close method is called when connection is null.
     */
    @Test
    public void testRestConnection_withNullConnection() throws Exception {
        Whitebox.invokeMethod(_snowflakeBrowser, "closeAndResetConnectionIfRequired",
                _snowflakeConnection,(Connection) null);

        Mockito.verify(_connection, Mockito.never()).getCatalog();
        Mockito.verify(_connection, Mockito.never()).getSchema();
        Mockito.verify(_connection, Mockito.never()).setCatalog(Mockito.anyString());
        Mockito.verify(_connection, Mockito.never()).setSchema(Mockito.anyString());
        Mockito.verify(_connection, Mockito.never()).close();
    }

    @Test
    public void testRestConnection_withSQLExceptionDuringClose() throws Exception {
        PropertyMap connectionProperties = createPropertyMap("TEST_SF_DB", "SCHEMA", true);
        connectionProperties.put("enablePooling", true);
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE,true);

        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_snowflakeConnection.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.doThrow(new SQLException("Close failed")).when(_connection).close();

        try {
            Whitebox.invokeMethod(_snowflakeBrowser, "closeAndResetConnectionIfRequired",
                    _snowflakeConnection, _connection);
            Assert.fail("Expected ConnectorException to be thrown");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Unable to close Snowflake connection"));
        }

        Mockito.verify(_connection, Mockito.times(1)).setCatalog(Mockito.anyString());
        Mockito.verify(_connection, Mockito.times(1)).setSchema(Mockito.anyString());
        Mockito.verify(_connection, Mockito.times(1)).close();
    }

    /**
     * Test behavior when the connection is null during the connection setup.
     * Verifies that no close method is called when override is disabled.
     */
    @Test
    public void testRestConnection_withNoOverrideConnection() throws Exception {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put(SnowflakeOverrideConstants.ENABLECONNECTIONOVERRIDE,false);

        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Whitebox.invokeMethod(_snowflakeBrowser, "closeAndResetConnectionIfRequired",
                _snowflakeConnection,_connection);

        Mockito.verify(_connection, Mockito.never()).getCatalog();
        Mockito.verify(_connection, Mockito.never()).getSchema();
        Mockito.verify(_connection, Mockito.never()).setCatalog(Mockito.anyString());
        Mockito.verify(_connection, Mockito.never()).setSchema(Mockito.anyString());
        Mockito.verify(_connection, Mockito.times(1)).close();
    }

    /**
     * Tests the behavior of the getObjectTypes method for different operation types
     * including QUERY, GET, and custom operation types such as bulkLoad, copyIntoTable, etc.
     */
    @Test
    public void testGetObjectTypesForQuery() throws SQLException {
        getObjectsFlow();
        //Query
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.QUERY);
        ObjectTypes objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        //GET
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.GET);
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        //No OperationType
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        // bulkLoad
        Mockito.when(_context.getCustomOperationType()).thenReturn("bulkLoad");
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        // copyIntoTable
        Mockito.when(_context.getCustomOperationType()).thenReturn("copyIntoTable");
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        // bulkUnload
        Mockito.when(_context.getCustomOperationType()).thenReturn("bulkUnload");
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());

        // copyIntoLocation
        Mockito.when(_context.getCustomOperationType()).thenReturn("copyIntoLocation");
        objectTypes = _snowflakeBrowser.getObjectTypes();
        Assert.assertNotNull(objectTypes);
        Assert.assertTrue(objectTypes.getTypes().isEmpty());
    }

    /**
     * Tests the connection setup and connection-related behavior of the SnowflakeBrowser.
     * Mocks SnowflakeConnection and verifies the connection flow using testConnection method.
     */
    @Test
    public void testConnection() {
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(snowflakeConnection.getAWSAccessKey()).thenReturn(StringUtil.EMPTY_STRING);
        Mockito.when(snowflakeConnection.getAWSSecret()).thenReturn(StringUtil.EMPTY_STRING);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).testConnection();
        _snowflakeBrowser.testConnection();
    }

    /**
     * Tests the behavior of the testConnection method when an exception is thrown.
     * Verifies that a ConnectorException is expected when connection setup fails.
     */
    @Test(expected = ConnectorException.class)
    public void testConnectionWithException() {
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).testConnection();
        _snowflakeBrowser.testConnection();
    }

    /**
     * Tests the behavior of the getStoredProcedures method when an exception occurs.
     * Verifies that a ConnectorException is thrown when a SQLException is encountered during JDBC connection
     * metadata retrieval.
     */
    @Test(expected = ConnectorException.class)
    public void testGetStoredProceduresWithException() throws Exception {
        PropertyMap operationProperties = createPropertyMap("", "SCHEMA", true);
        PropertyMap connectionProperties = createPropertyMap("TEST_DB", "TEST_SCHEMA", true);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);

        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(_snowflakeConnection);
        Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenThrow(new SQLException());
        ObjectTypes returnField = Whitebox.invokeMethod(_snowflakeBrowser, "getStoredProcedures");
        Assert.assertNotNull(returnField);
        Assert.assertEquals(0L, (long) returnField.getTypes().size());
    }

    /**
     * Tests the behavior of the getObjectDefinitions method for the CREATE operation type.
     * Verifies that the object definitions are retrieved correctly when the operation type is CREATE.
     */
    @Test
    public void testGetObjectDefinitionsForCreate() throws SQLException, JsonProcessingException {
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.CREATE);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectDefinitions(ArgumentMatchers.anyString(),
                ArgumentMatchers.any());
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        PowerMockito.when(SnowflakeOperationUtil.getObjectMapper()).thenReturn(mockObjectMapper);
        PowerMockito.when(mockObjectMapper.writeValueAsString(ArgumentMatchers.any())).thenReturn("");
        ObjectDefinitions definitions = _snowflakeBrowser.getObjectDefinitions("EMP", new ArrayList<>());
        Assert.assertNotNull(definitions);
        Assert.assertEquals("json", definitions.getDefinitions().get(0).getOutputType().value());
    }

    /**
     * Unit test for the {@code setCookieData} method in the {@code SnowflakeBrowser} class.
     * This test verifies the correct functionality of the method when setting metadata,
     * table metadata, and default values for a specific object type and schema.
     *
     * @throws SQLException              If an SQL error occurs during the test execution.
     * @throws JsonProcessingException   If an error occurs while processing JSON data.
     */
    @Test
    public void testSetCookieDataMethod() throws SQLException, JsonProcessingException {
        SortedMap<String, String> metaData = new TreeMap<>();
        PropertyMap properties = Mockito.mock(PropertyMap.class);
        properties.put(SnowflakeOverrideConstants.DATABASE, "db");
        properties.put(SnowflakeOverrideConstants.SCHEMA, "schema");
        metaData.put("TABLE_NAME", "EMP");
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.CREATE);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectDefinitions(ArgumentMatchers.anyString(),
                ArgumentMatchers.any());
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        PowerMockito.when(SnowflakeOperationUtil.getObjectMapper()).thenReturn(mockObjectMapper);
        PowerMockito.when(mockObjectMapper.writeValueAsString(ArgumentMatchers.any())).thenReturn("");
        SnowflakeBrowser.setOverriddenDb("db");
        SnowflakeBrowser.setOverriddenSchema("schema");
        PowerMockito.when(SnowflakeOperationUtil.getTableMetadata(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(metaData);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).setCookieData(ArgumentMatchers.any(), ArgumentMatchers.any());
        Mockito.when(snowflakeConnection.getContext()).thenReturn(_context);
        Mockito.when(_context.getConnectionProperties()).thenReturn(properties);
        Mockito.when(_context.getOperationProperties()).thenReturn(properties);
        ObjectDefinitions definitions = _snowflakeBrowser.getObjectDefinitions("EMP", new ArrayList<>());

        Assert.assertNotNull(definitions);
        Mockito.verify(_snowflakeBrowser, Mockito.times(1)).setCookieData(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    /**
     * Tests the behavior of the getObjectDefinitions method for the GET operation type.
     * Verifies that object definitions are retrieved correctly when the operation type is GET and connection
     * override is disabled.
     */
    @Test
    public void testGetObjectDefinitionsForGET() throws SQLException {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("enableConnectionOverride", false);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.GET);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectDefinitions(ArgumentMatchers.anyString(),
                ArgumentMatchers.any());
        ObjectDefinitions definitions = _snowflakeBrowser.getObjectDefinitions("EMP", new ArrayList<>());
        Assert.assertNotNull(definitions);
        Assert.assertEquals("json", definitions.getDefinitions().get(0).getOutputType().value());
    }

    /**
     * Tests the behavior of the getObjectDefinitions method for the QUERY operation type with document batching
     * enabled.
     * Verifies that object definitions are retrieved correctly when batching is enabled in the operation properties.
     */
    @Test
    public void testGetObjectDefinitionsForQueryWithBatching() throws SQLException {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("enableConnectionOverride", false);
        operationProperties.put("documentBatching", true);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.QUERY);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectDefinitions(ArgumentMatchers.anyString(),
                ArgumentMatchers.any());
        ObjectDefinitions definitions = _snowflakeBrowser.getObjectDefinitions("EMP", new ArrayList<>());
        Assert.assertNotNull(definitions);
        Assert.assertEquals("json", definitions.getDefinitions().get(0).getOutputType().value());
    }

    /**
     * Tests the behavior of the getObjectDefinitions method for the QUERY operation type with document batching
     * enabled.
     * Verifies that object definitions are retrieved correctly for a fully qualified object name (e.g., part1.part2
     * .part3.part4.part5).
     */
    @Test
    public void testGetObjectDefinitionsForQuery() throws SQLException {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("enableConnectionOverride", false);
        operationProperties.put("documentBatching", true);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.QUERY);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getColumns(ArgumentMatchers.any(), ArgumentMatchers.any(),
                ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectDefinitions(ArgumentMatchers.anyString(),
                ArgumentMatchers.any());
        ObjectDefinitions definitions = _snowflakeBrowser.getObjectDefinitions("part1.part2.part3.part4.part5",
                new ArrayList<>());
        Assert.assertNotNull(definitions);
        Assert.assertEquals("json", definitions.getDefinitions().get(0).getOutputType().value());
    }

    public void getObjectsFlow() throws SQLException {
        PropertyMap connectionProperties= new MutablePropertyMap();
        connectionProperties.put("db","DB");
        connectionProperties.put("schema","SCHEMA");
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("enableConnectionOverride",false);
        SnowflakeConnection snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
        Mockito.when(_snowflakeBrowser.getContext()).thenReturn(_context);
        Mockito.when(_context.getConnectionProperties()).thenReturn(connectionProperties);
        Mockito.when(_context.getOperationProperties()).thenReturn(operationProperties);
        Mockito.when(_snowflakeBrowser.getConnection()).thenReturn(snowflakeConnection);
        Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(_resultSet);
        Mockito.doCallRealMethod().when(_snowflakeBrowser).getObjectTypes();
    }
}
