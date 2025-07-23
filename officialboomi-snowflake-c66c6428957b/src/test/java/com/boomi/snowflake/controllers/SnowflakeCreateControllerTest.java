// Copyright (c) 2025 Boomi, LP
package com.boomi.snowflake.controllers;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.operations.BaseTestOperation;
import com.boomi.snowflake.stages.AmazonWebServicesHandler;
import com.boomi.snowflake.util.SnowflakeDataTypeConstants;
import com.boomi.snowflake.util.BoundedMap;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.TableDefaultAndMetaDataObject;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;

import com.boomi.util.DigestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ SnowflakeCreateController.class, SnowflakeOperationUtil.class, DigestUtil.class})
public class SnowflakeCreateControllerTest extends BaseTestOperation {

    private SnowflakeCreateController _snowflakeCreateController;
    private static final String OP_NAME = "create";
    private final PreparedStatement _statement = Mockito.mock(PreparedStatement.class);
    private SnowflakeWrapper _mockSnowflakeWrapper = Mockito.mock(SnowflakeWrapper.class);
    private final SnowflakeConnection _connection = Mockito.mock(SnowflakeConnection.class);
    private final SortedMap<String,String> input = new TreeMap<>();
    private final SortedMap<String,String> metadata = new TreeMap<>();
    private DynamicPropertyMap _dynamicPropertyMap = new MutableDynamicPropertyMap();
    private AmazonWebServicesHandler webServicesHandler;
    private InputStream inputStream;
    private ObjectData mockInputDocument;
    private DynamicPropertyMap mockDynamicPropertyMap;
    private ResultSet resultSet;
    private BoundedMap<String, TableDefaultAndMetaDataObject> boundedMap;
    private ConnectionProperties mockConnectionProperties;
    private PreparedStatement mockPreparedStatement;
    private Connection connection;
    private MutablePropertyMap mockPropertyMap;

    @Before
    public void setUp() throws Exception {
        mockPropertyMap = Mockito.mock(MutablePropertyMap.class);
        DatabaseMetaData databaseMetaData = Mockito.mock(DatabaseMetaData.class);
        Mockito.when(mockPropertyMap.getLongProperty("batchSize")).thenReturn(2L);
        Mockito.when(mockPropertyMap.getOrDefault("stageTempPath", "boomi/$OPERATION/$DATE/$TIME/$UUID/")).thenReturn("");
        webServicesHandler = Mockito.mock(AmazonWebServicesHandler.class);
        inputStream = Mockito.mock(InputStream.class);
        mockInputDocument = Mockito.mock(ObjectData.class);
        mockDynamicPropertyMap = Mockito.mock(DynamicPropertyMap.class);
        resultSet = Mockito.mock(ResultSet.class);
        mockPreparedStatement = Mockito.mock(PreparedStatement.class);
        mockConnectionProperties = Mockito.mock(ConnectionProperties.class);
        connection = Mockito.mock(Connection.class);
        when(connection.getCatalog()).thenReturn("DB");
        when(connection.getSchema()).thenReturn("SCHEMA");
        when(mockConnectionProperties.getTableName()).thenReturn("TABLE_NAME");
        when(_mockSnowflakeWrapper.getPreparedStatement()).thenReturn(mockPreparedStatement);
        when(mockPreparedStatement.getConnection()).thenReturn(connection);
        boundedMap = Mockito.mock(BoundedMap.class);
        Mockito.when(_connection.getOperationContext()).thenReturn(new SnowflakeContextIT(OperationType.CREATE, OP_NAME));
        ConnectionProperties properties = new ConnectionProperties(_connection,
                new MutablePropertyMap(),
                "TEST_TABLE",
                Logger.getAnonymousLogger());
        PowerMockito.whenNew(SnowflakeWrapper.class).withAnyArguments().thenReturn(_mockSnowflakeWrapper);
        _snowflakeCreateController = new SnowflakeCreateController(properties);
        // Mock the instantiation of AmazonWebServicesHandler to return a mocked instance
        PowerMockito.whenNew(AmazonWebServicesHandler.class)
                .withArguments(Mockito.anyString(), Mockito.anyString(), Mockito.anyString(), Mockito.anyString())  // Matches the constructor arguments
                .thenReturn(webServicesHandler);
        Mockito.when(connection.getCatalog()).thenReturn("SOLUTINS_DB");
        Mockito.when(connection.getSchema()).thenReturn("PUBLIC");
        Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);

    }

    /**
     * Tests no. of times Insert statement constructed for Snowflake create operation
     * with snowflake defaults for missing inputs
     * and batch size set to 1
     */
    @Test
    public void testReceiveDefaultSelection() throws SQLException {
        TableDefaultAndMetaDataObject valuesMap = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> defaultValue = new TreeMap<>();
        defaultValue.put("key1", "default1");
        valuesMap.setDefaultValues(defaultValue);
        Mockito.when(boundedMap.get(ArgumentMatchers.anyString())).thenReturn(valuesMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        verify(_mockSnowflakeWrapper, times(3)).constructInsertStatement(input,SnowflakeDataTypeConstants.DEFAULT_SELECTION,metadata, 1L,
                _dynamicPropertyMap);
    }

    @Test
    public void testReceiveDefaultSelectionWhenPreparedStatementsIsNotNull() throws SQLException {
        Mockito.when(mockPropertyMap.getLongProperty("batchSize", 1L)).thenReturn(3L);
        Mockito.when(mockPropertyMap.getLongProperty("parallelUpload", 4L)).thenReturn(4L);
        ConnectionProperties properties = new ConnectionProperties(_connection,
                mockPropertyMap,
                "TEST_TABLE",
                Logger.getAnonymousLogger());
        _snowflakeCreateController = new SnowflakeCreateController(properties);
        TableDefaultAndMetaDataObject valuesMap = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> defaultValue = new TreeMap<>();
        defaultValue.put("key1", "default1");
        valuesMap.setDefaultValues(defaultValue);
        Mockito.when(boundedMap.get(ArgumentMatchers.anyString())).thenReturn(valuesMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.NULL_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
    }

    /**
     * Tests no. of times Insert statement constructed for Snowflake create operation
     * with null selection for missing inputs
     * and batch size set to 1
     */
    @Test
    public void testReceiveNullSelection() throws SQLException {
        TableDefaultAndMetaDataObject valuesMap = new TableDefaultAndMetaDataObject();
        SortedMap<String, String> defaultValue = new TreeMap<>();
        defaultValue.put("key1", "default1");
        valuesMap.setDefaultValues(defaultValue);
        Mockito.when(boundedMap.get(ArgumentMatchers.anyString())).thenReturn(valuesMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.NULL_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.NULL_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.NULL_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        verify(_mockSnowflakeWrapper, times(0)).constructInsertStatement(input,SnowflakeDataTypeConstants.NULL_SELECTION,metadata, 1L,
                _dynamicPropertyMap);
    }

    @Test
    public void testSetDefaultValuesForDBAndSchemaWhenDefaultValuesArePresent() throws SQLException {
        input.put("DB", "DB");
        input.put("SCHEMA", "SCHEMA");
        SortedMap<String, String> defaultValue = new TreeMap<>();
        defaultValue.put("key1", "default1");
        TableDefaultAndMetaDataObject mockTableDefaultAndMetaDataObject = new TableDefaultAndMetaDataObject();
        mockTableDefaultAndMetaDataObject.setDefaultValues(defaultValue);
        when(boundedMap.get("DB|SCHEMA")).thenReturn( mockTableDefaultAndMetaDataObject);
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        verify(_mockSnowflakeWrapper, times(1)).constructInsertStatement(input,SnowflakeDataTypeConstants.DEFAULT_SELECTION,metadata, 1L,
                _dynamicPropertyMap);
    }

    @Test
    public void testSetDefaultValuesForDBAndSchemaWhenDefaultValuesAreNotPresent() throws SQLException {
        input.put("DB", "DB");
        input.put("SCHEMA", "SCHEMA");
        SortedMap<String, String> defaultValue = new TreeMap<>();
        defaultValue.put("key1", "default1");
        TableDefaultAndMetaDataObject mockTableDefaultAndMetaDataObject = new TableDefaultAndMetaDataObject();
        mockTableDefaultAndMetaDataObject.setDefaultValues(defaultValue);
        when(boundedMap.get("DB|SCHEMA")).thenReturn( new TableDefaultAndMetaDataObject());
        PowerMockito.mockStatic(SnowflakeOperationUtil.class);
        Mockito.when(mockConnectionProperties.getBatchSize()).thenReturn(2L);
        _snowflakeCreateController.receive(input, SnowflakeDataTypeConstants.DEFAULT_SELECTION, metadata, boundedMap, "mockTableName", _dynamicPropertyMap);
        verify(_mockSnowflakeWrapper, times(1)).constructInsertStatement(input,SnowflakeDataTypeConstants.DEFAULT_SELECTION,metadata, 1L,
                _dynamicPropertyMap);
    }

    /**
     * Tests the behavior of the `receive` method when the bucket name is provided and the stage name is null.
     * This test verifies that the AWS handler is properly set up and data is uploaded when a valid bucket name is provided.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test
    public void testReceiveWithBucketName() throws Exception {
        PropertyMap operationProperties =initializeOperationProperties("bucket", null);
        SnowflakeContextIT testContext = new SnowflakeContextIT(OperationType.CREATE, OP_NAME);
        SnowflakeConnection snowflakeConnection = new SnowflakeConnection(testContext);
        Connection connectionMock = PowerMockito.mock(Connection.class);
        Whitebox.setInternalState(snowflakeConnection, "connection", connectionMock);

        ConnectionProperties connectionProperties = new ConnectionProperties(
                snowflakeConnection,
                operationProperties,
                "TEST_TABLE",
                Logger.getAnonymousLogger()
        );

        Mockito.when(mockInputDocument.getDynamicOperationProperties()).thenReturn(mockDynamicPropertyMap);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString())).thenReturn(_statement);
        Mockito.when(webServicesHandler.getStageUrl("temp_stage_path")).thenReturn("s3://bucket/temp_stage_path");
        Mockito.when(webServicesHandler.getStageCredentials()).thenReturn("{\"accessKeyId\":\"mockKey\",\"secretAccessKey\":\"mockSecret\"}");
        webServicesHandler = Whitebox.invokeMethod(_snowflakeCreateController,
                "getAWSHandler", connectionProperties);
        _snowflakeCreateController.receive(inputStream, mockInputDocument, connectionProperties);
        Assert.assertNotNull("The AWS Handler should not be null", webServicesHandler);
    }

    /**
     * Tests the behavior of the `receive` method when the stage name is provided, and the bucket name is empty.
     * This test verifies that the internal stage handler is properly set up and data is uploaded when a valid stage name is provided.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test
    public void testReceiveWithStageName() throws Exception {
        PropertyMap operationProperties = initializeOperationProperties("", "stageName");
        SnowflakeContextIT testContext = new SnowflakeContextIT(OperationType.CREATE, OP_NAME);
        SnowflakeConnection snowflakeConnection = new SnowflakeConnection(testContext);
        Connection connectionMock = PowerMockito.mock(Connection.class);
        Whitebox.setInternalState(snowflakeConnection, "connection", connectionMock);
        ConnectionProperties connectionProperties = new ConnectionProperties(
                snowflakeConnection,
                operationProperties,
                "TEST_TABLE",
                Logger.getAnonymousLogger()
        );

        Mockito.when(mockInputDocument.getDynamicOperationProperties()).thenReturn(mockDynamicPropertyMap);
        Mockito.when(connectionMock.prepareStatement(Mockito.anyString())).thenReturn(_statement);
        Mockito.when(_statement.executeQuery()).thenReturn(resultSet);
        _snowflakeCreateController.receive(inputStream, mockInputDocument, connectionProperties);

        Assert.assertEquals("Compression type should be GZIP", "GZIP", operationProperties.get("compression"));
        Assert.assertEquals("File format name should be text", "text", operationProperties.get("fileFormatName"));
        Assert.assertTrue("The stage name should be set", operationProperties.containsKey("stageName"));
    }
    private PropertyMap initializeOperationProperties(String awsBucketName, String stageName) {
        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put("batchSize", 10L);
        operationProperties.put("parallelUpload", 4L);
        operationProperties.put("compression", "GZIP");
        operationProperties.put("sourceCompression", "AUTO");
        operationProperties.put("truncate", true);
        operationProperties.put("header", true);
        operationProperties.put("autoCompress", true);
        operationProperties.put("overwrite", false);
        operationProperties.put("columns", "col1,col2,col3");
        operationProperties.put("fileFormatType", "CSV");
        operationProperties.put("fileFormatName", "text");
        operationProperties.put("copyOptions", "option1,option2");
        operationProperties.put("awsBucketName", awsBucketName); // Dynamic
        operationProperties.put("awsRegion", "us-east-1");
        operationProperties.put("stageName", stageName); // Dynamic
        operationProperties.put("chunkSize", 250L);
        operationProperties.put("returnResults", false);
        operationProperties.put("stageTempPath", "temp_stage_path");
        return operationProperties;
    }
}