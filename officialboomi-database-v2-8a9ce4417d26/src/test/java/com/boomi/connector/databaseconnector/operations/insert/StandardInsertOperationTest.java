// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleOperationResult;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
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


public class StandardInsertOperationTest {

    private static final String SELECT_QUERY = "SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)";
    private static final String INSERT_SUBJECT_QUERY =
            "Insert into subject (SID,Semister,Marks,EnrolledDate,Sname,Subject,duration,fee,isqualified,stime,misc,restrictions)"
                    + "values (?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    private static final String INPUT_JSON_NULL_VALUE =
            "{\"SID\":30,\"Semister\":null,\"Marks\":null,\"EnrolledDate\":null,\"Sname\":null,"
                    + "\"Subject\":\"Biology\",\"duration\":null,\"fee\":null,\"isqualified\":null,\"stime\":null,"
                    + "\"misc\":null,\"restrictions\":\"null\"}";

    private static final String INPUT_NULL = "";
    private static final String OBJECT_TYPE_ID = "CUSTOMER";
    private final static String COLUMN_NAME_NAME = "name";
    private static final String ERROR_MESSAGE = "Response status is not SUCCESS";
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final OperationContext            _operationContext            = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final Connection                  _connection                  = Mockito.mock(Connection.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);

    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetInsert = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final List<ObjectType> _list = Mockito.mock(ArrayList.class);
    private final Iterator<ObjectData> _objectDataIterator = Mockito.mock(Iterator.class);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final StandardInsertOperation standardInsertOperation = new StandardInsertOperation(_databaseConnectorConnection);

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private Method method;
    private Class[] parameterTypes = new Class[5];
    private Object[] parameters = new Object[5];

    @Before
    @SuppressWarnings("java:S3011")
    public void setup() throws NoSuchMethodException {

        Mockito.when(_updateRequest.iterator()).thenReturn(_objectDataIterator);
        Mockito.when(_objectDataIterator.hasNext()).thenReturn(true, false);
        Mockito.when(standardInsertOperation.getContext()).thenReturn(_operationContext);

        InputStream result = new ByteArrayInputStream(DataTypesUtil.INPUT.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(11, result);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);

        parameterTypes[0] = Connection.class;
        parameterTypes[1] = JsonNode.class;
        parameterTypes[2] = Map.class;
        parameterTypes[3] = PreparedStatement.class;
        parameterTypes[4] = String.class;
        method = standardInsertOperation.getClass().getDeclaredMethod("prepareStatement", parameterTypes);
        method.setAccessible(true);

    }

    @Test
    public void testExecuteCreateOperationByProfile() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForUnknownDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCreateOperationByRowForMysql() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCreateOperationByRowBatchNotEqual() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCreateOperationByRowForMssqlServer() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMssqlDatabase();

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteOperationCommitByProfileInsert() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByProfilePostGre() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForPostGreDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByProfileJsonNullForOtherDatabase() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForPostGreDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);

        SimpleTrackedData trackedData = new SimpleTrackedData(11, null);
        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByProfileJsonNullAndIndexNullForOracle() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForProfile();

        Mockito.when(_connection.prepareStatement(Mockito.anyString(), (String[]) Mockito.any())).thenReturn(_preparedStatement);

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2).thenReturn(2);
        Mockito.when(_resultSet.isBeforeFirst()).thenReturn(true);
        Mockito.when(_resultSet.getMetaData().getColumnCount()).thenReturn(1);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME))
                .thenReturn(COLUMN_NAME_NAME).thenReturn(COLUMN_NAME_NAME);

        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        SimpleTrackedData trackedData = new SimpleTrackedData(11, null);
        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByProfileJsonNullAndIndexForOracle() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForProfile();

        Mockito.when(_connection.prepareStatement(Mockito.anyString(), (String[]) Mockito.any())).thenReturn(_preparedStatement);

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2).thenReturn(2);
        Mockito.when(_resultSet.isBeforeFirst()).thenReturn(false);
        Mockito.when(_resultSet.getMetaData().getColumnCount()).thenReturn(1);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME))
                .thenReturn(COLUMN_NAME_NAME).thenReturn(COLUMN_NAME_NAME);

        Mockito.when(_resultSet.next()).thenReturn(false);
        SimpleTrackedData trackedData = new SimpleTrackedData(11, null);
        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowOracle() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForRow();

        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_operationContext.getOperationProperties().getProperty(Mockito.anyString(), Mockito.anyString())).
                thenReturn(SELECT_QUERY);

        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteForBooleanDatatype() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForPostGreDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);

        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileJsonDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileNvarcharDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileClobDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetClob(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileStringDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }


    @Test
    public void testCommitByProfileIntegerDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForInteger(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileFloatDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetFloat(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileTimestampDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetTimestamp(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testCommitByProfileDateDataType() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForMysqlDatabaseForProfile();

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("6");

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(2);
        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testPrepareStatementForNullNode()
            throws InvocationTargetException, IllegalAccessException, SQLException, IOException {

        ObjectMapper objectMapper = new ObjectMapper();

        JsonNode _jsonNode = objectMapper.readTree(INPUT_JSON_NULL_VALUE);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn("MySQL");

        Map<String, String> _dataTypeMap = new HashMap();
        _dataTypeMap.put("SID", "integer");
        _dataTypeMap.put("Semister", "integer");
        _dataTypeMap.put("Marks", "float");
        _dataTypeMap.put("EnrolledDate", "date");
        _dataTypeMap.put("Sname", "string");
        _dataTypeMap.put("Subject", "string");
        _dataTypeMap.put("duration", "long");
        _dataTypeMap.put("fee", "double");
        _dataTypeMap.put("isqualified", "boolean");
        _dataTypeMap.put("stime", "time");
        _dataTypeMap.put("misc", "BLOB");
        _dataTypeMap.put("restrictions", "string");

        parameters[0] = _connection;
        parameters[1] = _jsonNode;
        parameters[2] = _dataTypeMap;
        parameters[3] = _preparedStatement;
        parameters[4] = INSERT_SUBJECT_QUERY;
        method.invoke(standardInsertOperation, parameters);

        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(2, Types.INTEGER);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(3, Types.FLOAT);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(4, Types.DATE);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(5, Types.VARCHAR);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(7, Types.BIGINT);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(8, Types.DECIMAL);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(9, Types.BOOLEAN);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(10, Types.TIME);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setNull(11, Types.BLOB);

        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(1, Types.INTEGER);
        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(6, Types.VARCHAR);
        Mockito.verify(_preparedStatement, Mockito.times(0)).setNull(12, Types.VARCHAR);
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
        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForProfile();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput("{\"price\":99999999999999999999999999999999999999}", _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericInteger(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal("99999999999999999999999999999999999999"), actual);
    }

    private void testExecuteBigValue(String input, String expected) throws SQLException {
        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForProfile();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        DataTypesUtil.setupInput(input, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(Mockito.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(expected), actual);
    }

    private void setupDataForExecuteInsertOperation() throws SQLException {

        DataTypesUtil.setupOperationContextObject(_operationContext, _propertyMap, OBJECT_TYPE_ID);

        DataTypesUtil.setUpConnectionObject(_connection, _databaseMetaData, _preparedStatement, _databaseConnectorConnection);

        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap, _list, OBJECT_TYPE_ID);

        DataTypesUtil.setUpResultObjectData(_resultSet);

        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet, _resultSetMetaData, new int[1]);
    }

    private void setPropertyObjectForProfile() {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.when(_operationContext.getOperationProperties().getProperty(Mockito.anyString(), Mockito.anyString())).
                thenReturn(SELECT_QUERY);
    }

    private void setPropertyDataForRow() {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_operationContext.getOperationProperties().getProperty(Mockito.anyString(), Mockito.anyString())).
                thenReturn(SELECT_QUERY);
    }

    private void setObjectForUnknownDatabase() throws SQLException {
        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn("UNKNOWN");
        setPropertyObjectForProfile();

    }

    private void setObjectForMssqlDatabase() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        setPropertyDataForRow();
    }

    private void setObjectForMysqlDatabaseForProfile() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setPropertyObjectForProfile();
    }

    private void setObjectForOracleDatabaseForRow() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setPropertyDataForRow();
    }

    private void setObjectForOracleDatabaseForProfile() throws SQLException {

        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setPropertyObjectForProfile();
    }

    private void setObjectForPostGreDatabase() throws SQLException {
        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setPropertyObjectForProfile();
    }

    /**
     *
     * Test Unique constraint exception
     * @throws SQLException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws SQLException {
        setupDataForExecuteInsertOperation();
        setObjectForOracleDatabaseForRow();

        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_operationContext.getOperationProperties().getProperty(Mockito.anyString(), Mockito.anyString())).
                thenReturn(SELECT_QUERY);

        DataTypesUtil.setupInput(DataTypesUtil.INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSet);

        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test to check if payload and result is success for commit by profile
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardInsertPlayloadSuccessByProfile() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForUnknownDatabase();
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "Ok",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"Query\":\"SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)\",\"Rows Effected\":0,\"Inserted Id\":[0],\"Status\":\"Executed Successfully\"}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));

    }

    /**
     * Test to check if payload and result is success for commit by row
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardInsertPlayloadSuccessByRow() throws SQLException {

        setupDataForExecuteInsertOperation();
        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn("UNKNOWN");
        setPropertyDataForRow();
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertNull("Message assert failed",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1,\"Inserted Ids \":[]}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is success for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardInsertPlayloadSuccessByRowMultiDocs() throws SQLException {

        setupDataForExecuteInsertMultipleDocs();
        setPropertyDataForRow();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        int playloadIndex = 1;

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertNull("Message assert failed",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"Status \":\"Batch executed successfully\",\"Batch Number \":"+playloadIndex+",\"No of records in batch \":1,\"Inserted Ids \":[0]}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if payload and result is success for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteStandardInsertPlayloadSuccessByProfileMultiDocs() throws SQLException {

        setupDataForExecuteInsertMultipleDocs();
        setPropertyObjectForProfile();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        int playloadIndex = 1;

        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "Ok",
                    simpleOperationResult.getMessage());

            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"Query\":\"SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)\",\"Rows Effected\":0,\"Inserted Id\":[0],\"Status\":\"Executed Successfully\"}",
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
    public void testExecuteStandardInsertPlayloadFailureByProfile() throws SQLException {

        setupDataForExecuteInsertOperation();
        setObjectForUnknownDatabase();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

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
    public void testExecuteStandardInsertPlayloadFailureByRow() throws SQLException {

        setupDataForExecuteInsertOperation();
        Mockito.when(_connection.getMetaData().getDatabaseProductName()).thenReturn("UNKNOWN");
        setPropertyDataForRow();

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

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
    public void testExecuteStandardInsertPlayloadFailureByProfileMultiDocs() throws SQLException {

        setupDataForExecuteInsertMultipleDocs();
        setPropertyObjectForProfile();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
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
    public void testExecuteStandardInsertPlayloadFailureByRowMultiDocs() throws SQLException {

        setupDataForExecuteInsertMultipleDocs();
        setPropertyDataForRow();

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        Mockito.when(_preparedStatement.executeUpdate()).thenReturn(0);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        standardInsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
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
     * configure the dataset for multiple documents
     *
     * @throws SQLException sql exception
     */
    private void setupDataForExecuteInsertMultipleDocs() throws SQLException {
        DataTypesUtil.setupOperationContextObject(_operationContext, _propertyMap, OBJECT_TYPE_ID);
        DataTypesUtil.setUpConnectionObject(_connection, _databaseMetaData, _preparedStatement, _databaseConnectorConnection);
        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap, _list, OBJECT_TYPE_ID);
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetMultipleDocsOperation(_preparedStatement, _resultSetInsert, _resultSet, _resultSetMetaData, new int[1]);
    }
}