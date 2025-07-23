// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestConstants;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author arshadpathan.
 */
public class StandardOperationTest {

    private static final String SELECT_QUERY = "SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)";

    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";

    private static final String UPDATE_CUSTOMER_QUERY =  "UPDATE CUSTOMERS SET country=?, name=?, age=?, bdate=?, isSenior=?, height=?, SSN=?, restrictions=?  WHERE id=?";


    private static final String INPUT_NULL_VALUE_CHECK =
            "{\"country\":null,\"name\":null,\"age\":null,\"bdate\":null,\"isSenior\":null,\"height\":null,\"SSN\":null,\"restrictions\":\"null\", \"id\":1 }";

    private static final String INPUT_NULL = "";
    private static final String OBJECT_TYPE_ID = "EVENT";
    private final static String STATUS_ERROR_MESSAGE = "Response status is not SUCCESS";
    private final static String SCHEMA_NAME_REF = "Schema Name";
    private final static String DATABASE_NAME = "Microsoft SQL Server";
    private final static String CATALOG = "Catalog";
    private static final long BATCH_COUNT_LONG = 10L;
    private final OperationContext _operationContext = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final DatabaseMetaData _databaseMetaDataForSQL = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Iterator<ObjectData> _objectDataIterator = Mockito.mock(Iterator.class);
    private final int[] intArray = new int[1];
    private final StandardOperation _standardOperation = new StandardOperation(_databaseConnectorConnection);
    private final SimpleOperationResponse _simpleOperationResponse = new SimpleOperationResponse();
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final ResultSet _resultSetInsert = Mockito.mock(ResultSet.class);
    private final List<ObjectType> _list = Mockito.mock(ArrayList.class);

    private Method method;

    private Class[] parameterTypes = new Class[5];

    private Object[] parameters = new Object[5];

    @Before
    public void setup() {

        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);

        Mockito.when(_standardOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(BATCH_COUNT_LONG);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.SCHEMA_NAME)).thenReturn(SCHEMA_NAME_REF);
        Mockito.when(_propertyMap.get(ArgumentMatchers.anyString())).thenReturn(SCHEMA_NAME_REF);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_databaseConnectorConnection.getReadTimeOut()).thenReturn(BATCH_COUNT_LONG);
        Mockito.when(_updateRequest.iterator()).thenReturn(_objectDataIterator);
        Mockito.when(_objectDataIterator.hasNext()).thenReturn(true, false);
    }

    /**
     * Test to check correctness of response SUCCESS status and payload with commit by rows
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteStatementsCommitByRows() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                "{\"Status \":\"Remaining records added to batch and executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check correctness of response SUCCESS status and payload with commit by rows equal to 1L
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteStatementsCommitByRowsBatchEquals() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check correctness of response status and payload with commit by profile with SUCCESS status
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteStatementsCommitByProfile() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Ok", operationMessage);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                "{\"Query\":\"query\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check correctness of response status and payload with commit by profile with FAILURE status
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteStatementsCommitByProfileFailure() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals("Response Status is not FAILURE", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException", operationMessage);
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResult.getPayloads().isEmpty());
    }

    /**
     * Test to check correctness of response FAILURE status and payload with commit by rows
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteStatementsCommitByRowsFailure() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals("Response Status is not FAILURE", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException", operationMessage);
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResult.getPayloads().isEmpty());
    }

    @Test
    public void testExecuteCommitByProfileJsonNull() throws SQLException {

        setupDataForExecuteOperation();

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);

        SimpleTrackedData trackedData = new SimpleTrackedData(11, null);
        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteForBooleanDatatype() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForPostGreDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileJsonDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileNvarcharDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileClobDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetClob(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileStringDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileIntegerDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetForInteger(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileFloatDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetFloat(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }


    @Test
    public void testCommitByProfileTimestampDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetTimestamp(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileDateDataType() throws SQLException {

        setupDataForExecuteForStandardOperation();
        setObjectForMysqlDatabaseProfile();

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("6");

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testPrepareStatementForNullNode()
            throws SQLException, IOException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {

        parameterTypes[0] = Connection.class;
        parameterTypes[1] = JsonNode.class;
        parameterTypes[2] = Map.class;
        parameterTypes[3] = PreparedStatement.class;
        parameterTypes[4] = String.class;
        method = _standardOperation.getClass().getDeclaredMethod("prepareStatement", parameterTypes);
        method.setAccessible(true);

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode _jsonNode = objectMapper.readTree(INPUT_NULL_VALUE_CHECK);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);

        Map<String, String> _dataTypeMap = new HashMap<>();

        _dataTypeMap.put("country", "nvarchar");
        _dataTypeMap.put("name", "string");
        _dataTypeMap.put("age", "integer");
        _dataTypeMap.put("bdate", "date");
        _dataTypeMap.put("isSenior", "boolean");
        _dataTypeMap.put("height", "double");
        _dataTypeMap.put("SSN", "long");
        _dataTypeMap.put("restrictions", "nvarchar");
        _dataTypeMap.put("id", "integer");

        parameters[0] = _connection;
        parameters[1] = _jsonNode;
        parameters[2] = _dataTypeMap;
        parameters[3] = _preparedStatement;
        parameters[4] = UPDATE_CUSTOMER_QUERY;
        method.invoke(_standardOperation, parameters);

        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(1, Types.NVARCHAR);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(2, Types.VARCHAR);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(3, Types.INTEGER);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(4, Types.DATE);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(5, Types.BOOLEAN);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(6, Types.DECIMAL);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(7, Types.BIGINT);

        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(8, Types.NVARCHAR);
    }

    /**
     *
     * Test Numeric data type precision for Oracle DB
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValue() throws SQLException {
        testExecuteBigValues(
                "{\"price\":999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}",
                "999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
    }

    /**
     *
     * Test Numeric data type precision for Oracle DB
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValueDecimals() throws SQLException {
        testExecuteBigValues("{\"price\":12345678901234567890123456789012345.12345678901234567890123456789}",
                "12345678901234567890123456789012345.12345678901234567890123456789");
    }

    /**
     *
     * Test Numeric data type precision for Oracle DB
     * @throws SQLException
     */
    @Test
    public void testExecuteIntegerBigValue() throws SQLException {
        setupDataForExecuteForStandardOperation();
        setObjectForOracleDatabaseProfile();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput("{\"price\":99999999999999999999999999999999999999}", _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericInteger(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal("99999999999999999999999999999999999999"), actual);
    }

    private void testExecuteBigValues(String input, String expected) throws SQLException {
        setupDataForExecuteForStandardOperation();
        setObjectForOracleDatabaseProfile();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput(input, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(expected), actual);
    }

    public void setupDataForExecuteOperation() throws SQLException {

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaDataForSQL);
        Mockito.when(_databaseMetaDataForSQL.getDatabaseProductName()).thenReturn(DATABASE_NAME);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);

        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_databaseMetaDataForSQL.getColumns(CATALOG, SCHEMA_NAME_REF, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);

        DataTypesUtil.setUpResultObjectData(_resultSet);

        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(intArray);

        InputStream result = new ByteArrayInputStream(DataTypesUtil.INPUT.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(11, result);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, _simpleOperationResponse);
    }

    private void setupDataForExecuteForStandardOperation() throws SQLException {

        DataTypesUtil.setupOperationContextObject(_operationContext, _propertyMap, OBJECT_TYPE_ID);

        DataTypesUtil.setUpConnectionObject(_connection, _databaseMetaData, _preparedStatement, _databaseConnectorConnection);

        DataTypesUtil.setUpResultObjectData(_resultSet);

        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap, _list, OBJECT_TYPE_ID);

        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet, _resultSetMetaData, intArray);

    }

    private void setObjectForMysqlDatabaseProfile() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setPropertyDataForProfile();
    }

    private void setObjectForOracleDatabaseProfile() throws SQLException {
        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setPropertyDataForProfile();
    }

    private void setObjectForPostGreDatabase() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setPropertyDataForProfile();
    }

    private void setPropertyDataForProfile() {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_operationContext.getOperationProperties().getProperty(Mockito.anyString(), Mockito.anyString())).
                thenReturn(SELECT_QUERY);
    }

    /**
     *
     * Test Unique constraint exception
     * @throws SQLException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws SQLException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);

        setupDataForExecuteOperation();
        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);
        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , _simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test to check if payload and result is Success for commit by row with single documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testSuccessStandardUpdatePayloadCommitByRows() throws SQLException, IOException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,null ,
                _simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE ,
                _simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                _simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,new String(_simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8),
                "{\"Status \":\"Remaining records added to batch and executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}");
    }

    /**
     * Test to check if payload and result is Success for commit by profile with single documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testSuccessStandardUpdatePayloadCommitByProfile() throws SQLException, IOException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE ,
                _simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE ,
                _simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                _simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,new String(_simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8),
                "{\"Query\":\"query\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}");
    }

    /**
     * Test to check if payload and result is Success for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadSuccessByRowMultiDocs() throws SQLException {
        setupDataForExecuteUpdateMultipleDocs();

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_propertyMap.getLongProperty(ArgumentMatchers.anyString())).thenReturn(1L);

        setupDataForExecuteOperation();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        int playloadIndex = 1;
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertNull(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    "{\"Status \":\"Batch executed successfully\",\"Batch Number \":"+playloadIndex+",\"No of records in batch \":1}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if payload and result is Success for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadSuccessByProfileMultiDocs() throws SQLException {
        setupDataForExecuteUpdateMultipleDocs();

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(DatabaseConnectorConstants.QUERY);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_propertyMap.getLongProperty(ArgumentMatchers.anyString())).thenReturn(1L);

        setupDataForExecuteOperation();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        int playloadIndex = 1;
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    "{\"Query\":\"query\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if payload and result is failure for commit by row with single documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadFailureByRow() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.FAILURE_ERROR_MESSAGE, OperationStatus.FAILURE,
                _simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "",
                _simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                _simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                _simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                _simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for commit by profile with single documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadFailureByProfile() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        setupDataForExecuteOperation();

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.FAILURE_ERROR_MESSAGE, OperationStatus.FAILURE,
                _simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "",
                _simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                _simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                _simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                _simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());

    }

    /**
     * Test to check if payload and result is failure for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadFailureByProfileMultiDocs() throws SQLException {
        setupDataForExecuteUpdateMultipleDocs();
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        setupDataForExecuteOperation();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);

        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.FAILURE_ERROR_MESSAGE, OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    simpleOperationResult.getPayloads().isEmpty());
        }
    }

    /**
     * Test to check if payload and result is failure for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardUpdatePayloadFailureByRowMultiDocs() throws SQLException {
        setupDataForExecuteUpdateMultipleDocs();
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        _standardOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.FAILURE_ERROR_MESSAGE, OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    simpleOperationResult.getPayloads().isEmpty());
        }
    }

    /**
     * configure the dataset for multiple documents
     *
     * @throws SQLException sql exception
     */
    private void setupDataForExecuteUpdateMultipleDocs() throws SQLException {
        DataTypesUtil.setupOperationContextObject(_operationContext, _propertyMap, OBJECT_TYPE_ID);
        DataTypesUtil.setUpConnectionObject(_connection, _databaseMetaData, _preparedStatement, _databaseConnectorConnection);
        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap, _list, OBJECT_TYPE_ID);
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetMultipleDocsOperation(_preparedStatement, _resultSetInsert, _resultSet, _resultSetMetaData, new int[1]);
    }
}
