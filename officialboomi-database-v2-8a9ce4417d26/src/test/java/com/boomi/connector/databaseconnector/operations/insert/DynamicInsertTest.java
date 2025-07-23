// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestContext;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DynamicInsertTest {

    private static final String INPUT = "{\"name\":\"Test\",\"id\":2,\"date\":\"2023-03-29\"}";
    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\"}";
    private static final String INPUT_THREE = "{\"time\":\"2018-09-01 09:01:15\"}";
    private static final String INPUT_WITH_NULL_NODE = "{\"name\":null,\"id\":2,\"date\":\"2023-03-29\"}";
    private static final String INPUT_BLOB =
            "{\"Shop\":{\"items\":{\"title\":\"Test_100375840415021414\",\"price\": 74.99,\"weight\":\"1300\","
                    + "\"quantity\":3,\"tax\":{\"price\":13.5,\"rate\":0.06,\"title\":\"tax\"}}}}";

    private final OperationContext            _operationContext            = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final UpdateRequest               _updateRequest               = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetInsert = Mockito.mock(ResultSet.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final List<ObjectType> _list = Mockito.mock(ArrayList.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);

    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final DynamicInsertOperation dynamicInsertOperation = new DynamicInsertOperation(
            _databaseConnectorConnection);

    @Before
    public void setup() throws SQLException {
        Mockito.when(dynamicInsertOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(DatabaseConnectorTestContext.OBJECT_TYPE_ID);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareStatement(Mockito.anyString(), Mockito.anyInt())).thenReturn(_preparedStatement);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);

        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(1L);
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");

        Mockito.when(_list.size()).thenReturn(2);

        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
        Mockito.when(_preparedStatement.getGeneratedKeys()).thenReturn(_resultSetInsert);

        Mockito.when(_resultSetInsert.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(0);

        Mockito.when(_databaseMetaData.getColumns(null, null, DatabaseConnectorTestContext.OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
    }

    private void setUpCommitByRows() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
    }

    private void setUpCommitByProfile() throws SQLException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
    }

    @Test
    public void testExecuteOperationCommitByProfileInsertMySQL() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setUpCommitByProfile();

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteOperationCommitByProfileInsert() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn("UNKNOWN");
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteOperationCommitByProfileInsertPostGre() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);

        setUpCommitByProfile();

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetLong(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteOperationCommitByProfileInteger() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setUpCommitByProfile();

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetFloat(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsJsonNull() throws SQLException {

        setUpCommitByRows();

        DataTypesUtil.setupInput(INPUT_THREE, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsFLoatNull() throws SQLException {

        setUpCommitByRows();

        DataTypesUtil.setupInput(INPUT_THREE, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetFloat(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsLongNull() throws SQLException {
        setUpCommitByRows();

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetLong(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsInsertBatchTwo() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(2L);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsDoubleNull() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsVarchar() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * This test is used to check When incoming data of type NVARCHAR is NULL, then connector to support and store Database NULL
     */
    @Test
    public void testExecuteCommitByRowsNvarcharNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT_WITH_NULL_NODE, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("name");

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsDateMySQL() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(3L);
        setUpCommitByRows();

        DataTypesUtil.setUpResultSetForDate(_resultSet);
        SimpleTrackedData data1 = new SimpleTrackedData(13, new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data = new SimpleTrackedData(2, new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.seUpTrackedDataList(_updateRequest, simpleOperationResponse,Arrays.asList(data1,data));
        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatusOne = simpleOperationResponse.getResults().get(0).getStatus();
        OperationStatus operationStatusTwo = simpleOperationResponse.getResults().get(1).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatusOne);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatusTwo);
    }

    @Test
    public void testExecuteCommitByRowsDateMySQLNull() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsDateOracle() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsInteger() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForInteger(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsIntegerNull() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_BLOB, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForInteger(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsTime() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForTime(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsTimeNull() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForTime(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsTimeStamp() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setUpResultSetTimestamp(_resultSet);
        DataTypesUtil.setupInput(INPUT_THREE, _updateRequest, simpleOperationResponse);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsTimeStampNull() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setUpResultSetTimestamp(_resultSet);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsString() throws SQLException {
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsBooleanNull() throws SQLException {
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsStringNull() throws SQLException {
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsBlob() throws SQLException {
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_BLOB, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBlob(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsBlobNull() throws SQLException {
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT_THREE, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBlob(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testForConnectorException() throws SQLException {
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(1L);
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.SCHEMA_NAME)).thenReturn("Schema Name");
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForIntegerForException(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsIntegerException() throws SQLException {

        setUpCommitByRows();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForIntegerForException(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsDateException() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(DatabaseConnectorConstants.BATCH_COUNT)).thenReturn(2L);

        setUpCommitByRows();

        SimpleTrackedData data1 = new SimpleTrackedData(13, new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data = new SimpleTrackedData(2, new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.seUpTrackedDataList(_updateRequest, simpleOperationResponse,Arrays.asList(data1,data));
        DataTypesUtil.setUpForDateException(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatusError = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR, operationStatusError);
    }

    /**
     *
     * Test Numeric data type precision for Oracle DB
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValue() throws SQLException {
        testExecuteBigValue(
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
        testExecuteBigValue("{\"price\":12345678901234567890123456789012345.12345678901234567890123456789}",
                "12345678901234567890123456789012345.12345678901234567890123456789");
    }

    /**
     *
     * Test Numeric data type precision for Oracle DB
     * @throws SQLException
     */
    @Test
    public void testExecuteIntegerBigValue() throws SQLException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput("{\"price\":99999999999999999999999999999999999999}", _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericInteger(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal("99999999999999999999999999999999999999"), actual);
    }

    private void testExecuteBigValue(String input, String expected) throws SQLException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput(input, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(expected), actual);
    }

    /**
     *
     * Test Unique constraint exception
     * @throws SQLException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws SQLException {
        DataTypesUtil.setConstraintExceptionOperation(_updateRequest, simpleOperationResponse,
                _preparedStatement, _resultSet,
                _databaseMetaData, _propertyMap);
        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test to check if payload and result is success for commit by profile
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadSuccessByProfile() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setUpCommitByProfile();

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "Ok",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"Query\":\"Insert into EVENT(name) values (?)\",\"Rows Effected\":2,\"Inserted Id\":[],\"Status\":\"Executed Successfully\"}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is success for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadSuccessByProfileMultiDocs() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_resultSetInsert.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        DataTypesUtil.setUpResultSetForVarChar(_resultSet);
        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"Query\":\"Insert into EVENT(id) values (?)\",\"Rows Effected\":0,\"Inserted Id\":[0],\"Status\":\"Executed Successfully\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Test to check if payload and result is success for commit by row
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadSuccessByRow() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "Ok",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is success for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadSuccessByRowMultiDocs() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        int playloadIndex = 1;

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"Status \":\"Batch executed successfully\",\"Batch Number \":"+playloadIndex+",\"No of records in batch \":1}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if payload and result is failure for commit by profile
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadFailureByProfile() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setUpCommitByProfile();

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue("Payload assert failed",
                simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for commit by row
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadFailureByRow() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue("Payload assert failed",
                simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadFailureByProfileMultiDocs() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_resultSetInsert.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue("Payload assert failed",
                    simpleOperationResult.getPayloads().isEmpty());

        }
    }

    /**
     * Test to check if payload and result is failure for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteDynamicInsertPlayloadFailureByRowMultiDocs() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue("Payload assert failed",
                    simpleOperationResult.getPayloads().isEmpty());
        }
    }

    /**
     * Tests the scenario when the table does not exist for the DynamicInsertOperation.
     *
     * @throws SQLException if an error occurs while interacting with the database
     */
    @Test
    public void testTableNotExists() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_resultSet.next()).thenReturn(false);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals("The specified table \"EVENT\" does not exist",
                simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * This test verifies that a string value containing special characters such as newline (`\n`) and backslashes (`\\`)
     * passed as NVARCHAR is correctly unescaped and inserted into the database without data loss or truncation.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs while reading the test data
     */
    @Test
    public void testExecuteCommitByRowsNVarcharWithString() throws SQLException, IOException {
        String jsonInputAsString = new String(Files.readAllBytes(Paths.get("src/test/resource/jsonInputAsString.json")));

        String expectedInvocationInput = "{\n" + "  \"invoice_req\": {\n" + "    \"trading_partner_code\": \"JSON as string input\",\n"
                + "    \"tp_bottler_code\": \"123\",\n" + "    \"tp_outlet_code\": \"987\",\n"
                + "    \"tp_po_number\": \"456789\"\n" + "  }\n" + "}";

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(jsonInputAsString, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Mockito.verify(_preparedStatement).setString(1, expectedInvocationInput);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }


    /**
     * This test verifies the correct handling of a JSON object passed as NVARCHAR
     * for insertion into the database, ensuring the object is stored without data loss.
     *
     * @throws SQLException if a database access error occurs
     * @throws IOException if an I/O error occurs while reading the test data
     */
    @Test
    public void testExecuteCommitByRowsNVarcharWithObject() throws SQLException, IOException {
        String jsonInputAsObject = new String(Files.readAllBytes(Paths.get("src/test/resource/jsonInputAsObject.json")));

        String expectedInvocationInput =
                "{trading_partner_code:Dynamic insert with direct json,tp_bottler_code:3,tp_outlet_code:981,"
                        + "tp_po_number:4567189,JSONRequest:{invoice_req:{trading_partner_code:ABC,"
                        + "tp_bottler_code:123,tp_outlet_code:987,tp_po_number:456789}}}";
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput(jsonInputAsObject, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Mockito.verify(_preparedStatement).setString(1, expectedInvocationInput);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }


    /**
     * This test verifies that a numeric value passed as NVARCHAR is correctly
     * inserted into the database without data loss or truncation.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testExecuteCommitByRowsNVarcharWithNumber() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput("{\"id\":4567189}", _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Mockito.verify(_preparedStatement).setString(1, "4567189");
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * This test verifies that a boolean value passed as NVARCHAR is correctly
     * inserted into the database without data loss or truncation.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testExecuteCommitByRowsNVarcharWithBoolean() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        DataTypesUtil.setupInput("{\"id\":true}", _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Mockito.verify(_preparedStatement).setString(1, "true");
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }
}