// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This Class is used to test {@link MysqlUpsert Class}
 */
public class MysqlUpsertTest {

    private static final String INPUT = "{\"name\":\"Test\",\"id\":2,\"date\":\"2023-03-29\"}";
    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\"}";
    private static final String INPUT_THREE = "{\"time\":\"2018-09-01 09:01:15\"}";
    private static final String INPUT_BLOB =
            "{\"Shop\":{\"items\":{\"title\":\"Test_100375840415021414\",\"price\": 74.99,\"weight\":\"1300\","
                    + "\"quantity\":3,\"tax\":{\"price\":13.5,\"rate\":0.06,\"title\":\"tax\"}}}}";

    private static final String COMMIT_BY_ROWS = "Commit By Rows";
    private static final String COMMIT_BY_PROFILE = "Commit By Profile";
    private static final String SCHEMA_NAME = "Schema Name";
    private static final String CATALOG = "catalog";
    private static final String CATALOG_ONE = "catalogOne";
    private static final String CATALOG_TWO = "catalogTwo";
    private static final String CATALOG_THREE = "catalogThree";
    private static final String OBJECT_TYPE_ID = "EVENT";
    private static final String CHECK_DATA_TYPE = "checkDataType";
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSet _resultSetMetaExtractor = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorOne = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorTwo = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorThree = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorFour = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorFive = Mockito.mock(ResultSet.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final PayloadMetadata _payloadMetadata = Mockito.mock(PayloadMetadata.class);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();

    private MysqlUpsert mysqlUpsert;
    private Method method;
    private Class[] parameterTypes = new Class[3];
    private Object[] parameters = new Object[3];

    @Before
    public void setup() throws SQLException {
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG).thenReturn(CATALOG_ONE).thenReturn(CATALOG_TWO).thenReturn(
                CATALOG_THREE).thenReturn(OBJECT_TYPE_ID).thenReturn(CHECK_DATA_TYPE);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);

        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);

        Mockito.when(_databaseMetaData.getColumns(CATALOG, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractor);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_ONE, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorOne);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_TWO, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorTwo);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_THREE, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorThree);
        Mockito.when(_databaseMetaData.getColumns(OBJECT_TYPE_ID, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorFour);
        Mockito.when(_databaseMetaData.getColumns(CHECK_DATA_TYPE, SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorFive);

        Mockito.when(_resultSetMetaExtractor.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorOne.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorTwo.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorThree.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorFour.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorFive.next()).thenReturn(true).thenReturn(false);

        parameterTypes[0] = PreparedStatement.class;
        parameterTypes[1] = ObjectData.class;
        parameterTypes[2] = Map.class;
    }

    private void setUpResultSetForInteger() throws SQLException {
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForString() throws SQLException {
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForJson() throws SQLException {
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorFive);
    }

    private MysqlUpsert getMysqlUpsert() {
        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, new HashSet<>());
        return mysqlUpsert;
    }

    /**
     * Tests the execution of statements for database operations.
     * Verifies that the statements are executed successfully with Oracle database
     * and returns the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsTest() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_ROWS, new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of statements with commit by profile option.
     * Verifies that the statements are executed successfully with Oracle database
     * and returns the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsCommitByProfileTest() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = getMysqlUpsert();

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForJson();

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of statements for MySQL upsert operation.
     * Verifies that the statements are executed successfully and returns
     * the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsShouldExecuteTest() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 2L, OBJECT_TYPE_ID, COMMIT_BY_ROWS, new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of batch statements with three records.
     * Verifies that multiple statements are executed successfully in batch mode
     * and returns the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsBatchCountThreeTest() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 3L, OBJECT_TYPE_ID, COMMIT_BY_ROWS, new LinkedHashSet<>());
        SimpleTrackedData data1 = new SimpleTrackedData(13,
                new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data = new SimpleTrackedData(2,
                new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data2 = new SimpleTrackedData(4,
                new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.seUpTrackedDataList(_updateRequest, simpleOperationResponse, Arrays.asList(data1, data, data2));
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of batch statements with two records.
     * Verifies that multiple statements are executed successfully in batch mode
     * and returns the expected success status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsBatchCountTwoTest() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 3L, OBJECT_TYPE_ID, COMMIT_BY_ROWS, new LinkedHashSet<>());
        SimpleTrackedData data1 = new SimpleTrackedData(13,
                new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data = new SimpleTrackedData(2,
                new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.seUpTrackedDataList(_updateRequest, simpleOperationResponse, Arrays.asList(data1, data));
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of statements with Integer data type handling.
     * Verifies that the operation successfully processes Integer type data
     * and returns the expected success status when using commit by profile.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileInteger() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForInteger();
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of statements with Integer and String data type handling.
     * Verifies that the operation successfully processes mixed data types
     * and returns the expected success status when using commit by profile.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileIntegerString() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, new HashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForString();
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of statements with Integer and Date data type handling.
     * Verifies that the operation successfully processes Integer and Date data types
     * and returns the expected success status when using commit by profile.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileIntegerDate() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorFive);
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Integer and Date data type handling.
     * Verifies that the operation successfully processes Integer and Date data types
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlIntegerDate() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Time data type handling.
     * Verifies that the operation successfully processes Time data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for time-based operations.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTime() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with NVChar data type handling.
     * Verifies that the operation successfully processes NVChar data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for variable-length character strings.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlNVChar() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);


        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, new HashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForVarChar(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Boolean data type handling.
     * Verifies that the operation successfully processes Boolean data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for boolean values.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBoolean() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Long data type handling.
     * Verifies that the operation successfully processes Long data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for long integer values.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlLong() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Float data type handling.
     * Verifies that the operation successfully processes Float data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for floating-point values.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlFloat() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Double data type handling.
     * Verifies that the operation successfully processes Double data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for double-precision floating-point values.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlDouble() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with BLOB (Binary Large Object) data type handling.
     * Verifies that the operation successfully processes BLOB data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for binary large object storage.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBlob() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_BLOB, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the MySQL upsert operation with Timestamp data type handling.
     * Verifies that the operation successfully processes Timestamp data type
     * and returns the expected success status. This test specifically validates
     * the MySQL database implementation for timestamp values.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTimestamp() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = getMysqlUpsert();
        DataTypesUtil.setupInput(INPUT_THREE, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorFive);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the behavior of buildInsertValues method when null values are passed as input.
     * This test verifies that the MySQL upsert operation correctly handles null values
     * in the input data stream and properly sets null values in the prepared statement.
     *
     * @throws SQLException if a database access error occurs
     * @throws NoSuchMethodException if the buildInsertValues method cannot be found
     * @throws InvocationTargetException if the method invocation fails
     * @throws IllegalAccessException if the method cannot be accessed
     */
    @Test
    @java.lang.SuppressWarnings("java:S3011")
    public void testNullSetWhenPassingInputAsNull()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Map<String, String> dataTypeMap = DataTypesUtil.getDataTypeMap();

        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(DataTypesUtil.INPUT_JSON_NULL_VALUE.getBytes(StandardCharsets.UTF_8)));

        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, DataTypesUtil.getLinkedHashSet());

        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;

        method = mysqlUpsert.getClass().getDeclaredMethod("buildInsertValues", parameterTypes);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);

        boolean testResult = DataTypesUtil.verifyTestForNullSetExecute(_preparedStatement);
        Assert.assertTrue(testResult);
    }

    /**
     *
     * Test Unique constraint exception
     * @throws SQLException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws SQLException {

        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_ROWS, new LinkedHashSet<>(
                Collections.singleton("name")));
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test to check that a properly formatted query is built and passed onto
     * {@link MysqlUpsert #commitByProfile(OperationResponse, int, Map, List, StringBuilder, PayloadMetadata)}
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteQueryValue() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, new LinkedHashSet<>(
                Collections.singleton("name")));

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForJson();

        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);

        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(_connection).prepareStatement(argumentCaptor.capture());
        String actual = argumentCaptor.getValue();
        String expected = "Insert into EVENT(name) values (?) ON DUPLICATE KEY UPDATE name=?";
        Assert.assertEquals(expected, actual);
    }

    /**
     * Tests the insertion of a string containing double quotes into MySQL database.
     * This test verifies that the buildInsertValues method correctly handles strings
     * with embedded double quotes.
     *
     * @throws SQLException              if a database access error occurs
     * @throws NoSuchMethodException     if the buildInsertValues method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithDoubleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\\"BeBoomi\\\"\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\"BeBoomi\"");
    }

    /**
     * Tests the insert operation with a string containing backslash character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithBackslash()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\\\BeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\\BeBoomi");
    }

    /**
     * Tests the insert operation with a string containing new line character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithNewLine()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\nBeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\nBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing tab character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithTab()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\tBeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\tBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing backspace character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithBackSpace()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\bBeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\bBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing carriage return character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithCarriageReturn()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\rBeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\rBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing form feed character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithFormFeed()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"BeBold\\fBeBoomi\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\fBeBoomi");
    }

    /**
     * Tests the insert operation with a string containing single quote character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithSingleQuote()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"I\'m testing singleQuote\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "I'm testing singleQuote");
    }

    /**
     * Tests the upsert (insert)  operation with a string containing comma character.
     *
     * @throws SQLException              if a database access error occurs
     * @throws IOException               if an I/O error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the underlying method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertComma()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson = "{\"field\":\"Test,Comma,\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "Test,Comma,");
    }

    /**
     * Tests the upsert (insert) operation with a string containing mixed escape characters.
     * This test verifies that special characters and escape sequences are properly handled
     * during the database update operation.
     * <p>
     * The test includes the following escape sequences:
     * - \t (tab)
     * - \b (backspace)
     * - \n (newline)
     * - \r (carriage return)
     * - \f (form feed)
     * - \' (single quote)
     * - \" (double quote)
     * - \\ (backslash)
     *
     * @throws SQLException              if a database access error occurs
     * @throws NoSuchMethodException     if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException    if the method is inaccessible
     */
    @Test
    public void testInsertStringWithMixedEscapeCharacters()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String inputJson =
                "{\"field\":\"Hello\\tWorld\\bNew\\nLine\\rCarriage\\fFormFeed\'Quote\\\"DoubleQuote\\\\Backslash\"}";
        setUpMySqlInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(mysqlUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Tests the upsert (update) operation with a string containing double quotes.
     * This test verifies that the MySQL connector properly handles string values
     * containing escaped double quotes during an update operation.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateDoubleQuoteString() throws SQLException {
        String inputJson = "{\"field\":\"24\\\"-26\\\"\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "24\"-26\"");
    }

    /**
     * Tests the upsert (update) operation with a string containing backslash character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithBackslash() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\\\BeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\\BeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing new line character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithNewLine() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\nBeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\nBeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing tab character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithTab() throws SQLException {

        String inputJson = "{\"field\":\"BeBold\\tBeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\tBeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing backspace character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithBackSpace() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\bBeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\bBeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing carriage return character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithCarriageReturn() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\rBeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\rBeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing form feed character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithFormFeed() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\fBeBoomi\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "BeBold\fBeBoomi");
    }

    /**
     * Tests the upsert (update) operation with a string containing single quote character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithSingleQuote() throws SQLException {
        String inputJson = "{\"field\":\"I\'m testing singleQuote\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "I'm testing singleQuote");
    }

    /**
     * Tests the upsert (update)  operation with a string containing comma character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateComma() throws SQLException {
        String inputJson = "{\"field\":\"Test,Comma,\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "Test,Comma,");
    }

    /**
     * Tests the upsert (update) operation with a string containing mixed escape characters.
     * This test verifies that special characters and escape sequences are properly handled
     * during the database update operation.
     * <p>
     * The test includes the following escape sequences:
     * - \t (tab)
     * - \b (backspace)
     * - \n (newline)
     * - \r (carriage return)
     * - \f (form feed)
     * - \' (single quote)
     * - \" (double quote)
     * - \\ (backslash)
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithMixedEscapeCharacters() throws SQLException {
        String inputJson =
                "{\"field\":\"Hello\\tWorld\\bNew\\nLine\\rCarriage\\fFormFeed\'Quote\\\"DoubleQuote\\\\Backslash\"}";
        setUpMySqlUpdateData(inputJson);
        createMySqlStringTypeTestConfig();
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Creates and configures a MysqlUpsert instance with test data for  insert operations.
     *
     * @param inputJson The JSON input string to be processed
     * @return CommonUpsert instance configured with the specified parameters
     * @throws NoSuchMethodException if the appendInsertParams method cannot be found
     */
    private void setUpMySqlInsertData(String inputJson) throws NoSuchMethodException, SQLException {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("field", "string");
        createMySqlStringTypeTestConfig();
        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8)));
        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;
        method = mysqlUpsert.getClass().getDeclaredMethod("buildInsertValues", parameterTypes);
    }

    /**
     * Sets up MySQL test data for update operations with string data types.
     *
     * @param inputJson The JSON input string containing test data
     * @throws SQLException If there is an error during database operations
     */
    private void setUpMySqlUpdateData(String inputJson) throws SQLException {
        createMySqlStringTypeTestConfig();
        DataTypesUtil.setupInput(inputJson, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractor);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("field");
    }

    /**
     * Sets up common test data for string data type operations in MySQL database tests.
     *
     * @return Map<String, String> A map containing field name to data type mapping
     * @throws SQLException If there is an error accessing the database metadata
     */
    private void createMySqlStringTypeTestConfig() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("field"));
        mysqlUpsert = new MysqlUpsert(_connection, 1L, OBJECT_TYPE_ID, COMMIT_BY_PROFILE, columnNames);
    }

    /**
     * Asserts that the MySQL upsert operation completed successfully.
     * Executes statements and verifies the operation status matches the expected success status.
     *
     * @throws SQLException if a database access error occurs
     */
    private void assertOperationStatus() throws SQLException {
        mysqlUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }
}
