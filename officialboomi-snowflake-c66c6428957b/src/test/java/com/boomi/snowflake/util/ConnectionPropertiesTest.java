// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.util.CollectionUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Logger;

import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

public class ConnectionPropertiesTest {

    private static final String PROP_STAGE_TMP_PATH = "stageTempPath";
    private static final String OP_NAME = "bulkLoad";
    private static final String CUSTOM_STAGE_PATH = "custom_stage/$OPERATION/$DATE/$TIME/$UUID";
    private static final String AWS_BUCKET_NAME = "awsBucketName";
    private static final String AWS_REGION = "awsRegion";
    private static final String STAGE_NAME = "stageName";
    private static final String FILE_PATH = "filePath";

    private ConnectionProperties _connectionProperties;
    private SnowflakeConnection _connection = Mockito.mock(SnowflakeConnection.class);

    private DynamicPropertyMap dynamicProperties;

    private final Logger mockLogger = Mockito.mock(Logger.class);

    @Before
    public void setup() {
        Mockito.when(_connection.getOperationContext()).thenReturn(
                new SnowflakeContextIT(OperationType.EXECUTE, OP_NAME));
        _connectionProperties = new ConnectionProperties(_connection, new MutablePropertyMap(), "TEST_TABLE",
                null);
    }

    @Test
    public void getStageTempPathCustomPath() {
        Map<String, String> dynamicProps = CollectionUtil.<String, String>mapBuilder().put(PROP_STAGE_TMP_PATH,
                CUSTOM_STAGE_PATH + "/").finishImmutable();
        ObjectData inputDoc = new SimpleTrackedData(1, null, null, dynamicProps);
        String stageTempPath = _connectionProperties.getStageTempPath(inputDoc);
        assertStagePath(parseStagePath(stageTempPath));
    }

    @Test
    public void getStageTempPathDefaultPath() {
        ObjectData inputDoc = new SimpleTrackedData(1, null, null, null);
        String stageTempPath = _connectionProperties.getStageTempPath(inputDoc);
        assertStagePath(parseStagePath(stageTempPath));
    }

    @Test
    public void getStageTempPathWithoutTrailingBackslash() {
        Map<String, String> dynamicProps = CollectionUtil.<String, String>mapBuilder().put(PROP_STAGE_TMP_PATH,
                CUSTOM_STAGE_PATH).finishImmutable();
        ObjectData inputDoc = new SimpleTrackedData(1, null, null, dynamicProps);
        String stageTempPath = _connectionProperties.getStageTempPath(inputDoc);
        assertThat(stageTempPath, endsWith("/"));
        assertStagePath(parseStagePath(stageTempPath));
    }

    private List<String> parseStagePath(String rawString) {
        String[] components = rawString.split("/");
        return Arrays.asList(components[1], components[2], components[3], components[4]);
    }

    private void assertStagePath(List<String> stagePath) {
        assertEquals(4, stagePath.size());
        assertEquals(OP_NAME, stagePath.get(0));
        assertThat(Integer.valueOf(stagePath.get(1)), any(Integer.class));
        assertThat(Double.valueOf(stagePath.get(2)), any(Double.class));
        assertThat(UUID.fromString(stagePath.get(3)), any(UUID.class));
    }

    /**
     * Test if dynamic operation properties set for S3 bucket name, AWS Region,
     * Internal Stage Name and Internal Source File Path
     * takes precedence over values set in operation context
     */
    @Test
    public void testDynamicOperationPropertiesValue(){
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(AWS_BUCKET_NAME, "dynamicBucket");
        dynamicOpProps.addProperty(AWS_REGION,"us-east-2");
        dynamicOpProps.addProperty(STAGE_NAME,"DynamicStage");
        dynamicOpProps.addProperty(FILE_PATH,"dynamicFile.csv");

        PropertyMap propertyMap = setDataForOperationProperties();
        ObjectData inputDoc = new SimpleTrackedData(1, null, null, null, dynamicOpProps);
        _connectionProperties = new ConnectionProperties(_connection, propertyMap, "TEST_TABLE", null);
        _connectionProperties.setDynamicProperties(inputDoc.getDynamicOperationProperties());

        assertEquals(dynamicOpProps.getProperty(AWS_BUCKET_NAME), _connectionProperties.getBucketName());
        assertEquals(dynamicOpProps.getProperty(AWS_REGION), _connectionProperties.getAWSRegion());
        assertEquals(dynamicOpProps.getProperty(STAGE_NAME), _connectionProperties.getStageName());
        assertEquals(dynamicOpProps.getProperty(FILE_PATH), _connectionProperties.getFilePath());
        assertNotEquals(propertyMap.getProperty(AWS_BUCKET_NAME), _connectionProperties.getBucketName());
        assertNotEquals(propertyMap.getProperty(FILE_PATH), _connectionProperties.getFilePath());

    }

    private PropertyMap setDataForOperationProperties(){
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put(AWS_BUCKET_NAME, "testBucket");
        propertyMap.put(AWS_REGION,"us-east-1");
        propertyMap.put(STAGE_NAME,"TEST_STAGE");
        propertyMap.put(FILE_PATH,"testFile.csv");
        return propertyMap;
    }

    /**
     * Tests the retrieval of a JDBC connection using dynamic properties.
     * Mocks the connection creation and verifies the connection getter's behavior.
     */
    @Test
    public void testGetConnection() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        Connection connection = Mockito.mock(Connection.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connection);
        connectionGetter.getConnection(mockLogger, dynamicProperties);
        ArgumentCaptor<String> catalogCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> schemaCaptor = ArgumentCaptor.forClass(String.class);

        Mockito.verify(connection).setCatalog(catalogCaptor.capture());
        Mockito.verify(connection).setSchema(schemaCaptor.capture());

        assertEquals("NEW DB", catalogCaptor.getValue());
        assertEquals("NEW SCHEMA", schemaCaptor.getValue());
    }

    /**
     * Tests the handling of SQLException in the getConnection method.
     * Verifies that a ConnectorException is thrown with the correct message and cause.
     */
    @Test
    public void testGetConnectionSQLExceptionHandling() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        String jdbcUrl = TestConfig.getJdbcUrl();
        String username = TestConfig.getUsername();
        String password = TestConfig.getPassword();
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Mockito.when(_connection.createJdbcConnection())
                .thenReturn(DriverManager.getConnection(jdbcUrl, username, password));
        try {
            connectionGetter.getConnection(mockLogger, dynamicProperties);
            Assert.fail("Expected ConnectorException was not thrown");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("Database access error"));
            Assert.assertTrue(e.getMessage().contains("Schema \"NEW SCHEMA\" not found"));
            Assert.assertTrue(e.getCause() instanceof SQLException);
        }
        Mockito.verify(_connection, Mockito.times(1)).createJdbcConnection();
    }

    /**
     * Tests reset Connection.
     * Reset with valid connection db and schema
     */
    @Test
    public void testResetConnection() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        Connection connectionMock = Mockito.mock(Connection.class);
        BrowseContext browseContext = Mockito.mock(BrowseContext.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Whitebox.setInternalState(_connectionProperties, "_connection", connectionMock);
        Whitebox.setInternalState(_connectionProperties, "_isOverrideEnabled", true);
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(connectionMock.getCatalog()).thenReturn("condb");
        Mockito.when(connectionMock.getSchema()).thenReturn("conschema");
        Mockito.when(_connection.getContext()).thenReturn(browseContext);
        Mockito.when(browseContext.getConnectionProperties())
                .thenReturn(createPropertyMap(true,"db","schema"));
        connectionGetter.close();
        Mockito.verify(connectionMock,Mockito.times(1)).close();
        Mockito.verify(connectionMock,Mockito.times(1)).setCatalog("DB");
        Mockito.verify(connectionMock,Mockito.times(1)).setSchema("SCHEMA");
    }

    /**
     * Tests reset Connection.
     * Reset with valid connection db and schema but same db and schemas
     */
    @Test
    public void testResetConnectionForSameDbAndSchema() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        Connection connectionMock = Mockito.mock(Connection.class);
        BrowseContext browseContext = Mockito.mock(BrowseContext.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Whitebox.setInternalState(_connectionProperties, "_connection", connectionMock);
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(connectionMock.getCatalog()).thenReturn("db");
        Mockito.when(connectionMock.getSchema()).thenReturn("schema");
        Mockito.when(_connection.getContext()).thenReturn(browseContext);
        Mockito.when(browseContext.getConnectionProperties())
                .thenReturn(createPropertyMap(true,"db","schema"));
        connectionGetter.close();
        Mockito.verify(connectionMock,Mockito.times(1)).close();
        Mockito.verify(connectionMock,Mockito.times(0)).setCatalog("db");
        Mockito.verify(connectionMock,Mockito.times(0)).setSchema("schema");
    }

    /**
     * Tests reset Connection.
     * Reset with  connection db and schema null
     */
    @Test
    public void testResetConnectionForSameDbAndSchemaNull() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        Connection connectionMock = Mockito.mock(Connection.class);
        BrowseContext browseContext = Mockito.mock(BrowseContext.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Whitebox.setInternalState(_connectionProperties, "_connection", connectionMock);
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(connectionMock.getCatalog()).thenReturn("db");
        Mockito.when(connectionMock.getSchema()).thenReturn("schema");
        Mockito.when(_connection.getContext()).thenReturn(browseContext);
        Mockito.when(browseContext.getConnectionProperties())
                .thenReturn(createPropertyMap(true,null,null));
        connectionGetter.close();
        Mockito.verify(connectionMock,Mockito.times(1)).close();
        Mockito.verify(connectionMock,Mockito.times(0)).setCatalog("db");
        Mockito.verify(connectionMock,Mockito.times(0)).setSchema("schema");
    }

    public static PropertyMap createPropertyMap(Boolean enabledPooling,String dbValue, String schemaValue) {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("enablePooling", enabledPooling);
        propertyMap.put(SnowflakeOverrideConstants.DATABASE, dbValue);
        propertyMap.put(SnowflakeOverrideConstants.SCHEMA, schemaValue);
        return propertyMap;
    }

    /**
     * Tests that an exception is thrown when attempting to reset the connection for the DB and schema.
     * Verifies that the expected ConnectorException is thrown during the operation.
     */
    @Test(expected = ConnectorException.class)
    public void testResetConnectionForSameDbAndSchemaThrowsException() throws SQLException {
        dynamicProperties = ConnectionOverrideUtilTest.createDynamicPropertyMap("new db",
                "new schema");
        Connection connectionMock = Mockito.mock(Connection.class);
        BrowseContext browseContext = Mockito.mock(BrowseContext.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Whitebox.setInternalState(_connectionProperties, "_connection", connectionMock);
        Whitebox.setInternalState(_connectionProperties, "_isOverrideEnabled", true);
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(connectionMock.getCatalog()).thenThrow(SQLException.class);
        Mockito.when(connectionMock.getSchema()).thenThrow(SQLException.class);
        Mockito.when(_connection.getContext()).thenReturn(browseContext);
        Mockito.when(browseContext.getConnectionProperties())
                .thenReturn(createPropertyMap(true,"db","schema"));
        connectionGetter.close();
    }

    /**
     * Tests reset Connection.
     * Reset with valid connection db and schema
     */
    @Test
    public void testResetConnectionWhenOverrideDisabled() throws SQLException {
        Connection connectionMock = Mockito.mock(Connection.class);
        BrowseContext browseContext = Mockito.mock(BrowseContext.class);
        ConnectionProperties.ConnectionGetter connectionGetter = _connectionProperties.new ConnectionGetter();
        Whitebox.setInternalState(_connectionProperties, "_connection", connectionMock);
        Whitebox.setInternalState(_connectionProperties, "_isOverrideEnabled", false);
        Mockito.when(_connection.createJdbcConnection()).thenReturn(connectionMock);
        Mockito.when(_connection.getContext()).thenReturn(browseContext);
        Mockito.when(browseContext.getConnectionProperties())
                .thenReturn(createPropertyMap(true,"db","schema"));
        connectionGetter.close();
        Mockito.verify(connectionMock,Mockito.times(1)).close();
        Mockito.verify(connectionMock,Mockito.never()).setCatalog("DB");
        Mockito.verify(connectionMock,Mockito.never()).setSchema("SCHEMA");
    }
}