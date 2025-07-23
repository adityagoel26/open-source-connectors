// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.ObjectTypes;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.OperationTypeConstants;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.COLUMN_NAME;
import static com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants.TYPE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseConnectorBrowserTest {

    private static final String OBJECT_TYPE_ID = "Object Type ID.";
    private static final String COLUMN_NAME1 = "COLUMN NAME";
    private static final String PROCEDURE_NAME = "Procedure Name";
    private static final String OBJECT_TYPE_ID1 = "Object Type ID";
    private static final String SCHEMA_NAME = "Schema Name";
    private static final String ENABLE_QUERY = "enableQuery";
    private static final String CUSTOM_OPS_TYPE = "Custom Ops Type";
    private static final String UPDATE_TYPE = "Update Type";
    private static final String DELETE_TYPE = "Delete Type";
    private static final String ORACLE_CATALOG = "Oracle Catalog";
    private static final String TABLE_NAMES = "tableNames";
    private static final String TABLE_NAME = "Table Name";
    private static final String MS_SQL_SERVER_CATALOG = "MsSQLServer Catalog";
    private static final String MY_SQL_CATALOG = "MySQL Catalog";
    private static final String DOCUMENT_BATCHING = "documentBatching";
    private static final String GET_TYPE = "Get Type";
    private static final String GET = "GET";
    private static final String CREATE = "CREATE";
    private static final String UPDATE = "UPDATE";
    private static final String DELETE = "DELETE";
    private final Connection _connection = mock(Connection.class);
    private final BrowseContext _browseContext = mock(BrowseContext.class);
    private final DatabaseMetaData _databaseMetaData = mock(DatabaseMetaData.class);
    private final PropertyMap _propertyMap = mock(PropertyMap.class);
    private final PreparedStatement _preparedStatement = mock(PreparedStatement.class);
    private final ResultSet _resultSet = mock(ResultSet.class);
    private final ResultSet _resultSet1 = mock(ResultSet.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = mock(DatabaseConnectorConnection.class);

    private final List<ObjectDefinitionRole> objectDefinitionRoleList = Arrays.asList(ObjectDefinitionRole.INPUT,
            ObjectDefinitionRole.OUTPUT);

    private final DatabaseConnectorBrowser _databaseConnectorBrowser = new DatabaseConnectorBrowser(
            _databaseConnectorConnection);

    private void setupForDatabaseConnectorBrowser(String schemaName) {

        when(_databaseConnectorBrowser.getContext()).thenReturn(_browseContext);
        when(_browseContext.getOperationProperties()).thenReturn(_propertyMap);
        when(_propertyMap.getProperty(DatabaseConnectorConstants.SCHEMA_NAME)).thenReturn(schemaName);
    }

    private void setupForDatabaseMetadata(String Oracle_Catalog, String mysql) throws SQLException {

        when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        when(_connection.getCatalog()).thenReturn(Oracle_Catalog);
        when(_databaseMetaData.getDatabaseProductName()).thenReturn(mysql);
    }

    private void setupForObjectDefinitionResultSet() throws SQLException {

        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);
        when(_resultSet.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME1);
        when(_resultSet.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void setupForProcedurePackageName() throws SQLException {

        when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME)).thenReturn(PROCEDURE_NAME);
        when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn(OBJECT_TYPE_ID1);
    }

    private void setupForObjectDefinitionProcedureWrite() throws SQLException {

        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);
        when(_databaseMetaData.getProcedureColumns(OBJECT_TYPE_ID1, null, "", null))
                .thenReturn(_resultSet);
        when(_resultSet.getString(COLUMN_NAME)).thenReturn(COLUMN_NAME1);
        when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn(OBJECT_TYPE_ID1);
        when(_resultSet.getShort(5)).thenReturn((short) 1);
        when(_resultSet.getString(TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
    }

    private void setupDataForDynamicGet() throws SQLException {
        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(CUSTOM_OPS_TYPE, UPDATE_TYPE, DELETE_TYPE, OperationTypeConstants.DYNAMIC_GET);
        setupForObjectDefinitionResultSet();

        when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        when(_propertyMap.getBooleanProperty(DOCUMENT_BATCHING, false)).thenReturn(false);
    }

    private void setupForPropertyMap(String operationType, String updateType, String deleteType, String getType)
            throws SQLException {

        when(_browseContext.getCustomOperationType()).thenReturn(operationType);
        when(_browseContext.getOperationType()).thenReturn(OperationType.GET);
        when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn(getType);
        when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn(updateType);
        when(_propertyMap.get(DatabaseConnectorConstants.DELETE_TYPE)).thenReturn(deleteType);
        when(_propertyMap.get(DatabaseConnectorConstants.INSERTION_TYPE)).thenReturn("Insert Type");
        when(_propertyMap.getBooleanProperty(ENABLE_QUERY, false)).thenReturn(true);
        when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
    }

    @Test
    public void testGetObjectDefinitionsWithDynamicInput() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(CUSTOM_OPS_TYPE, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionResultSet();

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                objectDefinitionRoleList);

        assertEquals(objectDefinitionRoleList.size(), actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsWithDynamicUpdate() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(CUSTOM_OPS_TYPE, OperationTypeConstants.DYNAMIC_UPDATE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionResultSet();

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                objectDefinitionRoleList);

        assertEquals(objectDefinitionRoleList.size(), actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsWithDynamicDelete() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(CUSTOM_OPS_TYPE, UPDATE_TYPE, OperationTypeConstants.DYNAMIC_DELETE, GET_TYPE);
        setupForObjectDefinitionResultSet();

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                objectDefinitionRoleList);

        assertEquals(objectDefinitionRoleList.size(), actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsWithDynamicGet() throws SQLException {

        setupDataForDynamicGet();

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.INPUT));

        assertEquals(Collections.singletonList(ObjectDefinitionRole.INPUT).size(),
                actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsOutputDynamicGet() throws SQLException {

        setupDataForDynamicGet();

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));

        assertEquals(Collections.singletonList(ObjectDefinitionRole.OUTPUT).size(),
                actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    /**
     * This test case is to test the scenario where the database throws an exception while fetching the data from the
     * database.
     * @throws SQLException
     */
    @Test
    public void testGetObjectDefinitionsOutputDynamicGetFailure() throws SQLException {

        setupDataForDynamicGet();
        when(_resultSet.next()).thenThrow(new SQLException("Database Exception occurred"));

        try {
            ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                    Collections.singletonList(ObjectDefinitionRole.OUTPUT));
            Assert.fail("Execution should throw an error");
        } catch (Exception e) {
            assertEquals("Database Exception occurred: java.sql.SQLException: Database Exception occurred",
                    e.getMessage());
        }
    }

    @Test
    public void testGetObjectDefinitionsWithStoredProcedureWrite() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(OperationTypeConstants.STOREDPROCEDUREWRITE, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionProcedureWrite();

        when(_resultSet.next()).thenReturn(false);
        when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet1);
        when(_resultSet1.next()).thenReturn(false);

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                objectDefinitionRoleList);

        assertEquals(objectDefinitionRoleList.size(), actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsWithGetForInput() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(GET, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionProcedureWrite();

        when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        when(_propertyMap.getBooleanProperty(DOCUMENT_BATCHING, false)).thenReturn(false);

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.INPUT));

        assertEquals(Collections.singletonList(ObjectDefinitionRole.INPUT).size(),
                actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectDefinitionsWithGetForOutput() throws SQLException {

        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(GET, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionProcedureWrite();

        when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        when(_propertyMap.getBooleanProperty(DOCUMENT_BATCHING, false)).thenReturn(false);

        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));

        assertEquals(Collections.singletonList(ObjectDefinitionRole.OUTPUT).size(),
                actualObjectDefinitions.getDefinitions().size());
        assertNotNull(actualObjectDefinitions.getOperationFields());
    }

    @Test
    public void testGetObjectTypesEqualsStoredProcedure() throws SQLException {

        String expectedObjectTypeId = "Object Type ID.Procedure Name";
        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        setupForDatabaseMetadata(ORACLE_CATALOG, DatabaseConnectorConstants.ORACLE);

        when(_browseContext.getCustomOperationType()).thenReturn(OperationTypeConstants.STOREDPROCEDUREWRITE);
        when(_databaseMetaData.getProcedures(ORACLE_CATALOG, null, "%")).thenReturn(_resultSet);

        setupForProcedurePackageName();
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(expectedObjectTypeId, actualObjectTypes.getTypes().get(0).getId());
    }

    @Test
    public void testGetObjectTypesIsObjectIdProcedureName() throws SQLException {

        String expectedObjectTypeId = PROCEDURE_NAME;
        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        setupForDatabaseMetadata(ORACLE_CATALOG, DatabaseConnectorConstants.MYSQL);

        when(_browseContext.getCustomOperationType()).thenReturn(OperationTypeConstants.STOREDPROCEDUREWRITE);
        when(_databaseMetaData.getProcedures(ORACLE_CATALOG, null, "%")).thenReturn(_resultSet);

        setupForProcedurePackageName();
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(expectedObjectTypeId, actualObjectTypes.getTypes().get(0).getId());
    }

    @Test
    public void testGetObjectTypesOperationTypeIsGet() throws SQLException {

        String expectedObjectTypeId = "TABLE NAME";
        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        when(_propertyMap.getProperty(TABLE_NAMES, null)).thenReturn(TABLE_NAME);
        setupForDatabaseMetadata(ORACLE_CATALOG, DatabaseConnectorConstants.ORACLE);

        when(_browseContext.getCustomOperationType()).thenReturn(GET);
        when(_connection.prepareStatement(anyString())).thenReturn(_preparedStatement);
        when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        when(_databaseMetaData.getTables(ORACLE_CATALOG, null, TABLE_NAME,
                new String[] { DatabaseConnectorConstants.TABLE, DatabaseConnectorConstants.VIEWS })).thenReturn(_resultSet1);
        when(_resultSet1.getString(DatabaseConnectorConstants.TABLE_NAME)).thenReturn(
                DatabaseConnectorConstants.TABLE_NAME);
        when(_resultSet1.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(expectedObjectTypeId, actualObjectTypes.getTypes().get(0).getId());
    }

    @Test
    public void testGetObjectTypesWhenTableNonNull() throws SQLException {

        String expectedObjectTypeId = TABLE_NAME;
        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        when(_propertyMap.getProperty(TABLE_NAMES, null)).thenReturn(TABLE_NAME);
        setupForDatabaseMetadata(ORACLE_CATALOG, DatabaseConnectorConstants.MYSQL);

        when(_connection.prepareStatement(anyString())).thenReturn(_preparedStatement);
        when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        when(_databaseMetaData.getTables(ORACLE_CATALOG, null, "`Table Name`",
                new String[] { DatabaseConnectorConstants.TABLE, DatabaseConnectorConstants.VIEWS })).thenReturn(
                _resultSet1);
        when(_resultSet1.getString(DatabaseConnectorConstants.TABLE_NAME)).thenReturn(
                DatabaseConnectorConstants.TABLE_NAME);
        when(_resultSet1.next()).thenReturn(true).thenReturn(false);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(expectedObjectTypeId, actualObjectTypes.getTypes().get(0).getId());
    }

    @Test
    public void testGetObjectTypesWhenMsSQLServerOperationTypeEqualsGet() throws SQLException {

        String expectedObjectTypeId = "PROCEDURE_NA";
        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        when(_browseContext.getCustomOperationType()).thenReturn(OperationTypeConstants.STOREDPROCEDUREWRITE);
        when(_propertyMap.getProperty(TABLE_NAMES, null)).thenReturn(TABLE_NAME);
        setupForDatabaseMetadata(MS_SQL_SERVER_CATALOG, DatabaseConnectorConstants.MSSQLSERVER);

        when(_databaseMetaData.getProcedures(MS_SQL_SERVER_CATALOG, DatabaseConnectorConstants.SCHEMA_NAME,
                "%")).thenReturn(_resultSet);
        when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME)).thenReturn(
                DatabaseConnectorConstants.PROCEDURE_NAME);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(expectedObjectTypeId, actualObjectTypes.getTypes().get(0).getId());
    }

    @Test
    public void testGetObjectTypesObjectTypeSetIdLabel() throws SQLException {

        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        setupForDatabaseMetadata(MY_SQL_CATALOG, DatabaseConnectorConstants.MYSQL);

        when(_databaseMetaData.getTables(MY_SQL_CATALOG, null, null,
                new String[] { DatabaseConnectorConstants.TABLE })).thenReturn(_resultSet);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(Types.CHAR, actualObjectTypes.getTypes().size());
    }

    @Test
    public void testGetObjectTypesWhenMYSQLOpTypeEqualsGet() throws SQLException {

        setupForDatabaseConnectorBrowser(DatabaseConnectorConstants.SCHEMA_NAME);
        when(_browseContext.getCustomOperationType()).thenReturn(DatabaseConnectorConstants.GET);
        setupForDatabaseMetadata(MY_SQL_CATALOG, DatabaseConnectorConstants.MYSQL);

        when(_databaseMetaData.getTables(MY_SQL_CATALOG, null, null,
                new String[] { DatabaseConnectorConstants.TABLE, DatabaseConnectorConstants.VIEWS })).thenReturn(
                _resultSet);
        when(_resultSet.next()).thenReturn(true).thenReturn(false);

        ObjectTypes actualObjectTypes = _databaseConnectorBrowser.getObjectTypes();

        assertEquals(Types.CHAR, actualObjectTypes.getTypes().size());
    }

    /**
     * Tests that the help text for SQL Get operations is correctly displayed.
     * This test verifies that when the operation type is set to Get, the correct
     * help text is provided for SQL prepared statements used in Get operations.
     *
     * <p>The expected help text instructs users on how to format SQL prepared statements
     * for Get operations, including guidance on handling multiple statements.</p>
     *
     * @throws SQLException if a database access error occurs
     */

    @Test
    public void testHelpTextGet() throws SQLException {
        String helpTextGet =
                "Type or paste a SQL prepared statement that is valid for the Get statement. For more than one "
                        + "statement, separate by semicolon and append a connection property allowMultiQueries=true "
                        + "to the database url.";
        setupDataForDynamicGet();
        setupForPropertyMap(GET, UPDATE_TYPE, DELETE_TYPE, OperationTypeConstants.GET);
        when(_browseContext.getOperationType()).thenReturn(OperationType.EXECUTE);
        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));
        assertEquals(helpTextGet, actualObjectDefinitions.getOperationFields().get(0).getHelpText());
    }

    /**
     * Tests that the help text for SQL insert operations is correctly displayed.
     * This test verifies that when the operation type is set to CREATE, the correct
     * help text is provided for SQL prepared statements used in insert operations.
     *
     * <p>The expected help text instructs users on how to format SQL prepared statements
     * for insert operations, including guidance on handling multiple statements.</p>
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testHelpTextInsert() throws SQLException {
        String helpTextInsert =
                "Type or paste a SQL prepared statement that is valid for the Insert statement. For more than one "
                        + "statement, separate by semicolon and append a connection property allowMultiQueries=true "
                        + "to the database url.";
        setupDataForDynamicGet();
        setupForPropertyMap(CREATE, UPDATE_TYPE, DELETE_TYPE, OperationTypeConstants.CREATE);
        when(_browseContext.getOperationType()).thenReturn(OperationType.CREATE);
        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));
        assertEquals(helpTextInsert, actualObjectDefinitions.getOperationFields().get(0).getHelpText());
    }

    /**
     * Tests that the help text for SQL Update operations is correctly displayed.
     * This test verifies that when the operation type is set to Update, the correct
     * help text is provided for SQL prepared statements used in Update operations.
     *
     * <p>The expected help text instructs users on how to format SQL prepared statements
     * for Update operations, including guidance on handling multiple statements.</p>
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testHelpTextUpdate() throws SQLException {
        String helpTextUpdate =
                "Type or paste a SQL prepared statement that is valid for the Update statement. For more than one "
                        + "statement, separate by semicolon and append a connection property allowMultiQueries=true "
                        + "to the database url.";
        setupDataForDynamicGet();
        setupForPropertyMap(UPDATE, UPDATE_TYPE, DELETE_TYPE, OperationTypeConstants.DYNAMIC_UPDATE);
        when(_browseContext.getOperationType()).thenReturn(OperationType.UPDATE);
        when(_browseContext.getCustomOperationType()).thenReturn(null);
        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));
        assertEquals(helpTextUpdate, actualObjectDefinitions.getOperationFields().get(0).getHelpText());
    }

    /**
     * Tests that the help text for SQL Delete operations is correctly displayed.
     * This test verifies that when the operation type is set to Delete, the correct
     * help text is provided for SQL prepared statements used in Delete operations.
     *
     * <p>The expected help text instructs users on how to format SQL prepared statements
     * for Delete operations, including guidance on handling multiple statements.</p>
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testHelpTextDelete() throws SQLException {
        String helpTextDelete =
                "Type or paste a SQL prepared statement that is valid for the Delete statement. For more than one "
                        + "statement, separate by semicolon and append a connection property allowMultiQueries=true "
                        + "to the database url.";
        setupDataForDynamicGet();
        setupForPropertyMap(DELETE, UPDATE_TYPE, DELETE_TYPE, OperationTypeConstants.DELETE);
        when(_browseContext.getOperationType()).thenReturn(OperationType.EXECUTE);
        ObjectDefinitions actualObjectDefinitions = _databaseConnectorBrowser.getObjectDefinitions(OBJECT_TYPE_ID,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT));
        assertEquals(helpTextDelete, actualObjectDefinitions.getOperationFields().get(0).getHelpText());
    }

    /**
     * Tests the getObjectDefinitions method when the custom operation type is set to STOREDPROCEDUREWRITE.
     * This test verifies that the method correctly handles stored procedure operations with RefCursor as out parameter
     * and returns the expected ObjectDefinitions.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testGetObjectDefinitionsWithRefCursorForPostgresql() throws SQLException {

        String jsonSchema = "{\"$schema\":\"http://json-schema.org/draft-07/schema#\"," +
                "\"type\":\"object\",\"properties\":{\""+COLUMN_NAME1+"\":{\"type\":\"array\"}}}";
        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(OperationTypeConstants.STOREDPROCEDUREWRITE, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionProcedureWrite();

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        Mockito.when(_databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                Mockito.any())).thenReturn(_resultSet);
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"));
        Mockito.when(_resultSet.getString(6)).thenReturn("2012");
        Mockito.when(_resultSet.getString(4)).thenReturn(COLUMN_NAME1);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<ObjectDefinitionRole> roles = Collections.singletonList(ObjectDefinitionRole.OUTPUT);
        ObjectDefinitions result = _databaseConnectorBrowser.getObjectDefinitions("test.testProcedure", roles);

        assertNotNull(result);
        assertNotNull(result.getDefinitions());
        assertEquals(jsonSchema, result.getDefinitions().get(0).getJsonSchema());

    }

    /**
     * Tests the getObjectDefinitions method when the custom operation type is set to STOREDPROCEDUREWRITE.
     * This test verifies that the method correctly handles stored procedure operations with RefCursor as out parameter
     * and returns the expected ObjectDefinitions.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testGetObjectDefinitionsWithRefCursorForOracle() throws SQLException {
        setupForDatabaseConnectorBrowser(SCHEMA_NAME);
        setupForPropertyMap(OperationTypeConstants.STOREDPROCEDUREWRITE, UPDATE_TYPE, DELETE_TYPE, GET_TYPE);
        setupForObjectDefinitionProcedureWrite();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                Mockito.any())).thenReturn(_resultSet);
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"));
        Mockito.when(_resultSet.getString(6)).thenReturn("2012");
        Mockito.when(_resultSet.getString(4)).thenReturn(COLUMN_NAME1);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        List<ObjectDefinitionRole> roles = Collections.singletonList(ObjectDefinitionRole.OUTPUT);
        ObjectDefinitions result = _databaseConnectorBrowser.getObjectDefinitions("test.testProcedure", roles);
        assertNotNull(result);
        assertNotNull(result.getDefinitions());
    }
}
