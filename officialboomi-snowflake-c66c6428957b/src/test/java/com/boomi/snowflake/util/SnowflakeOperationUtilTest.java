// Copyright (c) 2025 Boomi, LP.

package com.boomi.snowflake.util;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

/**
 * Unit tests for the {@link SnowflakeOperationUtil} class.
 */
public class SnowflakeOperationUtilTest {

    private Connection connection;
    private PreparedStatement mockPreparedStatement;
    private PreparedStatement mockPreparedStatement2;
    private DatabaseMetaData mockMetaData;
    private ResultSet mockResultSet1;
    private ResultSet mockResultSet2;
    private DynamicPropertyMap mockDynamicProperties;
    private SnowflakeWrapper mockSnowflakeWrapper;
    private SortedMap<String, String> input;
    private BoundedMap<String, TableDefaultAndMetaDataObject> boundedMap = new BoundedMap<>(5);
    private TableDefaultAndMetaDataObject defaultAndMetaDataObject = new TableDefaultAndMetaDataObject();
    private SortedMap<String, String> mockSetData;
    private ConnectionProperties mockConnectionProperties;
    private String cookieValue = "{\"TEST_SF_DB|PUBLIC\":{\"defaultValues\":{\"AVAILABLE\":\"TRUE\",\"DESC\":\"new product\",\"PRICE\":\"200\",\"RELEASE\":\"01012025\"},\"metaDataValues\":{\"AVAILABLE\":\"BOOLEAN\",\"CODE\":\"NUMBER\",\"DERIVED\":\"NUMBER\",\"DESC\":\"VARCHAR\",\"ID\":\"NUMBER\",\"NAME\":\"VARCHAR\",\"PRICE\":\"NUMBER\",\"RELEASE\":\"DATE\"},\"tableMetaDataValues\":{\"AVAILABLE\":\"BOOLEAN\",\"CODE\":\"NUMBER\",\"DESC\":\"VARCHAR\",\"ID\":\"NUMBER\",\"NAME\":\"VARCHAR\",\"PRICE\":\"NUMBER\",\"RELEASE\":\"DATE\"}}}";
    private SortedMap<String, String> defaultValues = new TreeMap<>();
    private SortedMap<String, String> metaDataValues = new TreeMap<>();
    TableDefaultAndMetaDataObject defaultValueObject = new TableDefaultAndMetaDataObject();
    private ObjectMapper mockObjectMapper;
    private TypeFactory mockTypeFactory;
    private Logger mockLogger;
    private ConnectionProperties.ConnectionGetter mockConnectionGetter;
    private PropertyMap mockPropertyMap;

    /**
     * Sets up the common mocks and test data before each test execution.
     *
     * @throws SQLException if a SQL error occurs during setup.
     */
    @Before
    public void setUp() throws SQLException {
        connection = Mockito.mock(Connection.class);
        mockLogger = Mockito.mock(Logger.class);
        mockPropertyMap = Mockito.mock(PropertyMap.class);
        mockConnectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
        mockPreparedStatement2 = Mockito.mock(PreparedStatement.class);
        mockMetaData = Mockito.mock(DatabaseMetaData.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        mockResultSet1 = Mockito.mock(ResultSet.class);
        mockResultSet2 = Mockito.mock(ResultSet.class);
        mockDynamicProperties = Mockito.mock(DynamicPropertyMap.class);
        mockSnowflakeWrapper = Mockito.mock(SnowflakeWrapper.class);
        mockConnectionProperties = Mockito.mock(ConnectionProperties.class);
        input = new TreeMap<>();
        mockSetData = new TreeMap<>();
        mockSetData.put("KEY_1", "VALUE_1");
        defaultAndMetaDataObject.setDefaultValues(mockSetData);
        input.put("NAME", "TEST_DATA");
        boundedMap.put("DIGEST_KEY", defaultAndMetaDataObject);
        metaDataValues.put("key1", "metaData");
        defaultValues.put("key1", "value1");
        defaultValueObject.setDefaultValues(defaultValues);
        defaultValueObject.setMetaDataValues(metaDataValues);
        defaultValueObject.setTableMetaDataValues(metaDataValues);
        mockObjectMapper = Mockito.mock(ObjectMapper.class);
        mockTypeFactory = Mockito.mock(TypeFactory.class);

        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet1);
        Mockito.when(mockResultSet1.next()).thenReturn(true, false); // Iterate once
        Mockito.when(connection.prepareStatement("SHOW COLUMNS IN MockTable")).thenReturn(mockPreparedStatement);
        Mockito.when(connection.prepareStatement("SELECT null")).thenReturn(mockPreparedStatement2);

        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet1);
        Mockito.when(mockPreparedStatement2.executeQuery()).thenReturn(mockResultSet2);
        Mockito.when(mockResultSet2.next()).thenReturn(true, false);

        Mockito.when(mockResultSet1.getString(SnowflakeOperationUtil.DEFAULT_VALUE_INDEX)).thenReturn("defaultValue");
        Mockito.when(mockResultSet1.getString(SnowflakeOperationUtil.COLUMN_NAME)).thenReturn("column1");
        Mockito.when(mockResultSet1.getString(4)).thenReturn("column1");
    }

    /**
     * Tests that {@link SnowflakeOperationUtil#getTableMetadataForCreate(String, Connection, String, String)}
     * correctly calls the underlying methods and returns the expected metadata map.
     *
     * @throws SQLException if a SQL error occurs.
     */
    @Test
    public void testGetTableMetadataForCreateCallsActualMethod() throws SQLException {
        Mockito.when(connection.getCatalog()).thenReturn("TestDB");
        Mockito.when(connection.getSchema()).thenReturn("TestSchema");
        Mockito.when(connection.getMetaData()).thenReturn(mockMetaData);
        Mockito.when(mockMetaData.getColumns("TestDB", "TestSchema", "TestTable", "%")).thenReturn(mockResultSet2);
        // Mock PreparedStatement and ResultSet for query execution
        Mockito.when(connection.prepareStatement("SHOW COLUMNS IN TestTable")).thenReturn(mockPreparedStatement);

        // Configure resultSet1 (query results)
        Mockito.when(mockResultSet1.next()).thenReturn(true, false);
        Mockito.when(mockResultSet1.getString(SnowflakeOperationUtil.COLUMN_KIND_INDEX)).thenReturn("COLUMN");
        Mockito.when(mockResultSet1.getString(SnowflakeOperationUtil.AUTOINCREMENT_INDEX)).thenReturn("");
        // Configure resultSet2 (metadata columns)
        Mockito.when(mockResultSet2.next()).thenReturn(true, true, false);
        Mockito.when(mockResultSet2.getString(SnowflakeOperationUtil.COLUMN_NAME_INDEX)).thenReturn("col1", "col2");
        Mockito.when(mockResultSet2.getString(SnowflakeOperationUtil.DATA_TYPE_INDEX)).thenReturn("VARCHAR", "INTEGER");
        // Call the method under test
        SortedMap<String, String> metadata = SnowflakeOperationUtil.getTableMetadataForCreate(
                "TestTable",
                connection,
                null,
                null
        );

        Assert.assertEquals(1, metadata.size());
        Assert.assertEquals("VARCHAR", metadata.get("col2"));
    }

    /**
     * Tests that {@link SnowflakeOperationUtil#getDefaultsValues(String, Connection, DynamicPropertyMap)}
     * correctly retrieves the default values from the table.
     *
     * @throws SQLException if a SQL error occurs.
     */
    @Test
    public void testGetDefaultsValues() throws SQLException {
        SortedMap<String, String> result = SnowflakeOperationUtil.getDefaultsValues(
                "MockTable",
                connection,
                mockDynamicProperties
        );
        Mockito.verify(connection).prepareStatement("SHOW COLUMNS IN MockTable");
        Mockito.verify(connection).prepareStatement("SELECT null");
        Mockito.verify(mockPreparedStatement, Mockito.times(1)).executeQuery();
        Mockito.verify(mockPreparedStatement2, Mockito.times(1)).executeQuery();

        Assert.assertNull(result.get("column1"));
    }

    /**
     * Tests that {@link SnowflakeOperationUtil#setDefaultValuesForDBAndSchema(SnowflakeWrapper, SortedMap, String, String, BoundedMap, DynamicPropertyMap, Long)}
     * properly sets the default values for the specified database and schema.
     *
     * @throws SQLException if a SQL error occurs.
     */
    @Test
    public void testSetDefaultValuesForDBAndSchema() throws SQLException {
        Mockito.when(connection.prepareStatement("SELECT null")).thenReturn(mockPreparedStatement2);
        Mockito.when(connection.prepareStatement("SHOW COLUMNS IN MockTable")).thenReturn(mockPreparedStatement);
        Mockito.when(mockSnowflakeWrapper.getPreparedStatement()).thenReturn(mockPreparedStatement);
        Mockito.when(mockPreparedStatement.getConnection()).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(mockMetaData);
        Mockito.when(mockMetaData.getURL()).thenReturn("https://mockURL");
        Mockito.when(connection.getCatalog()).thenReturn("SOLUTIONS_DB");
        Mockito.when(connection.getSchema()).thenReturn("PUBLIC");
        Mockito.when(connection.prepareStatement("SHOW COLUMNS IN mockTableName")).thenReturn(mockPreparedStatement);
        Mockito.when(mockPreparedStatement.executeQuery()).thenReturn(mockResultSet1);

        SnowflakeOperationUtil.setDefaultValuesForDBAndSchema(mockSnowflakeWrapper, input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, "mockTableName", boundedMap, mockDynamicProperties, 2L);
        Mockito.verify(mockPreparedStatement, Mockito.times(1)).executeQuery();
        Mockito.verify(mockPreparedStatement2, Mockito.times(1)).executeQuery();
    }

    /**
     * Tests that {@link SnowflakeOperationUtil#readDataFromCookie(String, ConnectionProperties)}
     * correctly reads and parses the cookie JSON string into a map.
     *
     * @throws JsonProcessingException if JSON parsing fails.
     */
    @Test
    public void testReadDataFromCookie() throws JsonProcessingException {
        SortedMap<String, TableDefaultAndMetaDataObject> expectedMap = new TreeMap<>();
        TableDefaultAndMetaDataObject defaultValueObject12 = new TableDefaultAndMetaDataObject();
        defaultValueObject12.setDefaultValues(defaultValues);
        defaultValueObject12.setMetaDataValues(metaDataValues);
        defaultValueObject12.setTableMetaDataValues(metaDataValues);
        expectedMap.put("SOLUTIONS|DB", defaultValueObject12);
        Mockito.when(mockObjectMapper.getTypeFactory()).thenReturn(mockTypeFactory);
        MapType mapType = TypeFactory.defaultInstance()
                .constructMapType(TreeMap.class, String.class, TableDefaultAndMetaDataObject.class);
        Mockito.when(mockTypeFactory.constructMapType(Mockito.eq(TreeMap.class), Mockito.eq(String.class), Mockito.eq(TableDefaultAndMetaDataObject.class))).thenReturn(mapType);
        Mockito.when(mockObjectMapper.readValue(cookieValue, mapType)).thenReturn(expectedMap);
        SortedMap<String, TableDefaultAndMetaDataObject> result = SnowflakeOperationUtil.readDataFromCookie(cookieValue, mockConnectionProperties);
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());
    }

    /**
     * Tests that {@link SnowflakeOperationUtil#getMetadataValues(ConnectionProperties, String, String, String)}
     * retrieves the metadata values by verifying that the correct database metadata methods are invoked.
     *
     * @throws SQLException if a SQL error occurs.
     */
    @Test
    public void testGetMetadataValues() throws SQLException {
        Mockito.when(mockConnectionProperties.getConnectionGetter()).thenReturn(mockConnectionGetter);
        Mockito.when(mockConnectionProperties.getLogger()).thenReturn(mockLogger);
        Mockito.when(mockConnectionGetter.getConnection(mockLogger)).thenReturn(connection);
        Mockito.when(connection.getMetaData()).thenReturn(mockMetaData);
        Mockito.when(mockMetaData.getColumns(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).thenReturn(mockResultSet1);
        SnowflakeOperationUtil.getMetadataValues(mockConnectionProperties, "TEST_TABLE", "SOLUTIONS_DB", "PUBLIC");
        Mockito.verify(connection).getMetaData();
        Mockito.verify(mockMetaData).getColumns(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString());
    }

    /**
     * processes the cookie and metadata correctly, updating the bounded map.
     *
     * @throws SQLException if a SQL error occurs.
     * @throws JsonProcessingException if JSON processing fails.
     */
    @Test
    public void testProcessCookieAndMetadata() throws SQLException, JsonProcessingException {
        Mockito.when(mockPropertyMap.getLongProperty("batchSize")).thenReturn(2L);
        SnowflakeOperationUtil.processCookieAndMetadata(mockPropertyMap, mockConnectionProperties, cookieValue, boundedMap);
        Assert.assertEquals(2, boundedMap.size());
    }
}
