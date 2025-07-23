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
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This Class is used to test {@link PostgresUpsert Class}
 */
public class PostgresUpsertTest {

    private static final String COMMIT_BY_ROWS = "Commit By Rows";
    private static final String INPUT =
            "{\r\n" + "\"id\":\"123\",\r\n" + "\"name\":\"abc" + "\",\r\n" + "\"date\":\"2019-12-09\",\r\n"
                    + "\"clob\":\"Hello\",\r\n" + "\"isqualified\":true,\r\n" + "\"laptime\":\"21:09:08\"\r\n" + " "
                    + " \r\n" + " }";
    private static final String INPUT_NULL =
            "{\r\n" + "\"id\":null,\r\n" + "\"name\":null,\r\n" + "\"date\":null,\r\n" + "\"clob\":null,\r\n"
                    + "\"isqualified\":null,\r\n" + "\"laptime\":null\r\n" + " " + " \r\n" + " }";
    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\"}";
    private static final String INPUT_TWO_NULL = "{\"time\":null,\"isValid\":null,\"phNo\":null,\"price\":null}";

    private static final String INPUT_TIME = "{\"time\":\"2018-09-01 09:01:15\"}";
    private static final String INPUT_TIME_NULL = "{\"time\":null}";

    private static final String INPUT_BLOB =
            "{\"Shop\":{\"items\":{\"title\":\"Test_100375840415021414\",\"price\": 74.99,\"weight\":\"1300\","
                    + "\"quantity\":3,\"tax\":{\"price\":13.5,\"rate\":0.06,\"title\":\"tax\"}}}}";
    private static final String INPUT_BLOB_NULL = "{\"Shop\":null}";

    private static final String OBJECT_TYPE_ID = "CUSTOMER";
    private static final String CATALOG = "catalog";
    private static final String SCHEMA = "SCHEMA";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_NAME_IS_QUALIFIED = "isqualified";
    private static final String CATALOG_ONE = "catalogOne";
    private static final String CATALOG_TWO = "catalogTwo";
    private static final String CATALOG_THREE = "catalogThree";
    private static final String CHECK_DATA_TYPE = "checkDataType";
    private final ResultSet _resultSetPrimaryKey = Mockito.mock(ResultSet.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractor = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorOne = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorTwo = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorThree = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorFour = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetMetaExtractorFive = Mockito.mock(ResultSet.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final PayloadMetadata _payloadMetadata = Mockito.mock(PayloadMetadata.class);

    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();

    private Method method;
    private Class[] parameterTypes = new Class[3];
    private Object[] parameters = new Object[3];
    private PostgresUpsert postgresUpsert;
    @Before
    public void setup() throws SQLException {

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_resultSet.next()).thenReturn(true, false);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, SCHEMA, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_resultSetPrimaryKey.isBeforeFirst()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);

        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_connection.getSchema()).thenReturn(DatabaseConnectorConstants.SCHEMA_NAME);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG).thenReturn(CATALOG_ONE).thenReturn(CATALOG_TWO).thenReturn(
                CATALOG_THREE).thenReturn(OBJECT_TYPE_ID).thenReturn(CHECK_DATA_TYPE);

        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);

        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractor);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_ONE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorOne);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_TWO, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorTwo);
        Mockito.when(_databaseMetaData.getColumns(CATALOG_THREE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorThree);
        Mockito.when(_databaseMetaData.getColumns(OBJECT_TYPE_ID, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorFour);
        Mockito.when(_databaseMetaData.getColumns(CHECK_DATA_TYPE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(
                _resultSetMetaExtractorFive);

        Mockito.when(_resultSetMetaExtractor.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorOne.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorTwo.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorThree.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorFour.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetMetaExtractorFive.next()).thenReturn(true).thenReturn(false);

        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractor);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG_ONE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractorOne);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG_TWO, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractorTwo);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG_THREE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractorThree);
        Mockito.when(_databaseMetaData.getPrimaryKeys(OBJECT_TYPE_ID, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractorFour);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CHECK_DATA_TYPE, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(
                _resultSetMetaExtractorFive);

        parameterTypes[0] = PreparedStatement.class;
        parameterTypes[1] = ObjectData.class;
        parameterTypes[2] = Map.class;
    }

    private void setUpData() throws SQLException {
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_connection.getSchema()).thenReturn(SCHEMA);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON)
                .thenReturn(DatabaseConnectorConstants.NVARCHAR);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, SCHEMA, OBJECT_TYPE_ID)).thenReturn(_resultSetPrimaryKey);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME).thenReturn(
                COLUMN_NAME_IS_QUALIFIED);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, SCHEMA, OBJECT_TYPE_ID)).thenReturn(_resultSetPrimaryKey);
    }

    private void setCommonUpsert() throws SQLException {
        Mockito.when(_databaseMetaData.getColumns(CATALOG, SCHEMA, OBJECT_TYPE_ID, null)).thenReturn(_resultSetMetaExtractor);
        Mockito.when(_resultSetMetaExtractor.next()).thenReturn(true, false);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_connection.getSchema()).thenReturn(SCHEMA);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, SCHEMA, OBJECT_TYPE_ID, null)).thenReturn(_resultSetMetaExtractor);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.NVARCHAR);
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, SCHEMA, OBJECT_TYPE_ID)).thenReturn(_resultSetPrimaryKey);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME)
                .thenReturn(COLUMN_NAME_IS_QUALIFIED);
    }

    private PostgresUpsert getPostgresUpsert(long batchCount) throws SQLException {
        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, batchCount, OBJECT_TYPE_ID, COMMIT_BY_ROWS,
                new LinkedHashSet<>());
        setUpData();

        int[] result = new int[] { 1, 2 };
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(result);

        InputStream inputStreamResult = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(inputStreamResult, _updateRequest, simpleOperationResponse);
        return postgresUpsert;
    }

    private void setUpResultSetForInteger() throws SQLException {
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetForInteger(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForJson() throws SQLException {
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetJson(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForString() throws SQLException {
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForFloat() throws SQLException {
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetFloat(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForDate() throws SQLException {
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetForDate(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForTime() throws SQLException {
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetForTime(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForBlob() throws SQLException {
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetBlob(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForDouble() throws SQLException {
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForLong() throws SQLException {
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetLong(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForTimestamp() throws SQLException {
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetTimestamp(_resultSetMetaExtractorFive);
    }

    private void setUpResultSetForBoolean() throws SQLException {
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractor);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorOne);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorTwo);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorThree);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorFour);
        DataTypesUtil.setUpResultSetBoolean(_resultSetMetaExtractorFive);
    }

    /**
     * Tests the execution of statements with commit by profile configuration for Postgres database.
     * This test verifies that the PostgresUpsert operation correctly processes the statements
     * and returns a successful operation status when using commit by profile setting.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void executeStatementsCommitByProfileTest() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForJson();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of commit by profile functionality with Integer data type handling.
     * This test verifies that the PostgresUpsert operation correctly processes Integer data type
     * when using commit by profile setting and returns a successful operation status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileInteger() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        setUpResultSetForInteger();
        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of commit by profile functionality with null Integer values.
     * This test verifies that the PostgresUpsert operation correctly handles null Integer values
     * when using commit by profile setting and returns a successful operation status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileIntegerNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);
        setUpResultSetForInteger();
        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of commit by profile functionality with String data type handling.
     * This test verifies that the PostgresUpsert operation correctly processes String data type
     * when using commit by profile setting and returns a successful operation status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileString() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);

        setUpResultSetForString();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of commit by profile functionality with null String values.
     * This test verifies that the PostgresUpsert operation correctly handles null String values
     * when using commit by profile setting and returns a successful operation status.
     * The test mocks the database as Oracle and validates the operation's behavior with null input.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileStringNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForString();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of PostgreSQL upsert operation with Date data type handling.
     * This test verifies that the PostgresUpsert operation correctly processes Date data type
     * when the database is configured as Oracle and returns a successful operation status.
     * The test sets up date-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlDate() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);

        setUpResultSetForDate();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of PostgreSQL upsert operation with null Date values.
     * This test verifies that the PostgresUpsert operation correctly handles null Date values
     * when the database is configured as Oracle and returns a successful operation status.
     * The test specifically validates the handling of null date inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlDateNull() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForDate();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Time data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Time data type
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up time-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTime() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);

        setUpResultSetForTime();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Time data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly handles null Time values
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null time inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTimeNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForTime();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Boolean data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Boolean data type
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up boolean-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBoolean() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);

        setUpResultSetForBoolean();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Boolean data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null Boolean values
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null boolean inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBooleanNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForBoolean();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Long data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Long data type
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up long-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlLong() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);

        setUpResultSetForLong();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Long data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null Long values
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null long inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlLongNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForLong();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Float data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Float data type
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up float-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlFloat() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);

        setUpResultSetForFloat();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Float data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null Float values
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null float inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlFloatNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForFloat();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Double data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Double data type
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up double-specific result sets and validates the operation's behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlDouble() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);

        setUpResultSetForDouble();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Double data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null Double values
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null double inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlDoubleNUll() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TWO_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForDouble();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL BLOB data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes BLOB (Binary Large Object) data
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up blob-specific result sets and validates the operation's behavior with binary data.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBlob() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_BLOB, _updateRequest, simpleOperationResponse);

        setUpResultSetForBlob();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL BLOB data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null BLOB (Binary Large Object) data
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null BLOB inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlBlobNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_BLOB_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForBlob();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Timestamp data type handling in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes Timestamp data
     * when the database is configured as MySQL and returns a successful operation status.
     * The test sets up timestamp-specific result sets and validates the operation's behavior
     * with temporal data.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTimestamp() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TIME, _updateRequest, simpleOperationResponse);

        setUpResultSetForTimestamp();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL Timestamp data type handling with null values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes null Timestamp data
     * when the database is configured as MySQL and returns a successful operation status.
     * The test specifically validates the handling of null timestamp inputs in the database operation.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteMySqlTimestampNull() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                new LinkedHashSet<>());
        DataTypesUtil.setupInput(INPUT_TIME_NULL, _updateRequest, simpleOperationResponse);

        setUpResultSetForTimestamp();

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10, DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of commit by profile common upsert operation in PostgreSQL.
     * This test verifies that the PostgresUpsert operation correctly processes data
     * when configured with commit by rows profile setting. The test mocks the database
     * connection, prepares statements, and validates that the operation completes
     * successfully with the expected operation status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testExecuteCommitByProfileCommonUpsert() throws SQLException {

        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        setCommonUpsert();

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 0L, OBJECT_TYPE_ID, COMMIT_BY_ROWS,
                new LinkedHashSet<>());

        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 1, SCHEMA, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the building of an insert query statement in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly constructs and
     * executes an insert query statement. The test validates that the operation
     * completes successfully with the expected operation status.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testBuildInsertQueryStatement() throws SQLException {

        PostgresUpsert postgresUpsert = getPostgresUpsert(1L);

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 1, SCHEMA, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the batch execution functionality in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly processes
     * a batch of statements and returns a successful operation status. The test
     * uses a PostgresUpsert instance with a specified batch size to validate
     * the batch processing behavior.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed connection
     */
    @Test
    public void testDoBatch() throws SQLException {

        PostgresUpsert postgresUpsert = getPostgresUpsert(12L);

        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 1, SCHEMA, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }


    /**
     * Tests the handling of null type setting when passing null input values in PostgreSQL upsert operation.
     * This test verifies that the PostgresUpsert operation correctly handles null values during parameter
     * setting in the prepared statement. The test uses a SimpleTrackedData instance with null values
     * and validates the proper type setting behavior.
     *
     * @throws SQLException if a database access error occurs
     * @throws NoSuchMethodException if the specified method cannot be found
     * @throws InvocationTargetException if the invoked method throws an exception
     * @throws IllegalAccessException if the method cannot be accessed
     */
    @Test
    @java.lang.SuppressWarnings("java:S3011")
    public void testNullTypeSetWhenPassingInputValueNull()
            throws SQLException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        Map<String, String> dataTypeMap = DataTypesUtil.getDataTypeMap();

        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(DataTypesUtil.INPUT_JSON_NULL_VALUE.getBytes(StandardCharsets.UTF_8)));

        PostgresUpsert postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID, DatabaseConnectorConstants.COMMIT_BY_PROFILE,
                DataTypesUtil.getLinkedHashSet());

        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;

        method = postgresUpsert.getClass().getDeclaredMethod("appendInsertPreapreStatement", parameterTypes);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);

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

        PostgresUpsert postgresUpsert = getPostgresUpsert(1L);
        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);
        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 1, SCHEMA, _payloadMetadata);

        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Tests the insertion of a string containing double quotes into Postgres database.
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
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
        setUpPostgresInsertData(inputJson);
        method.setAccessible(true);
        method.invoke(postgresUpsert, parameters);
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Tests the update operation for a string containing double quotes in PostgreSQL.
     * This test verifies that string values containing double quotes are properly escaped
     * and handled during an upsert operation.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateDoubleQuoteString() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        String inputJson = "{\"field\":\"24\\\"-26\\\"\"}";
        setUpPostGresUpdateData(inputJson);
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1, "24\"-26\"");
    }

    /**
     * Tests the Upsert (update) operation with a string containing backslash character.
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testUpdateStringWithBackslash() throws SQLException {
        String inputJson = "{\"field\":\"BeBold\\\\BeBoomi\"}";
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
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
        setUpPostGresUpdateData(inputJson);
        assertOperationStatus();
        Mockito.verify(_preparedStatement, Mockito.times(1)).setString(1,
                "Hello\tWorld\bNew\nLine\rCarriage\fFormFeed'Quote\"DoubleQuote\\Backslash");
    }

    /**
     * Asserts that the postgres upsert operation completed successfully.
     * Executes statements and verifies the operation status matches the expected success status.
     *
     * @throws SQLException if a database access error occurs
     */
    private void assertOperationStatus() throws SQLException {
        postgresUpsert.executeStatements(_updateRequest, simpleOperationResponse, 10,
                DatabaseConnectorConstants.SCHEMA_NAME, _payloadMetadata);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Sets up PostgreSQL insert data for testing purposes.
     *
     * @param inputJson The JSON input string containing the update data
     * @throws SQLException          If a database access error occurs
     * @throws NoSuchMethodException if the appendInsertPreapreStatement method cannot be found
     */
    private void setUpPostgresInsertData(String inputJson) throws NoSuchMethodException, SQLException {
        Map<String, String> dataTypeMap = new HashMap<>();
        dataTypeMap.put("field", "string");
        createPostgresStringTypeTestConfig();
        SimpleTrackedData trackedData = new SimpleTrackedData(13,
                new ByteArrayInputStream(inputJson.getBytes(StandardCharsets.UTF_8)));
        parameters[0] = _preparedStatement;
        parameters[1] = trackedData;
        parameters[2] = dataTypeMap;
        method = postgresUpsert.getClass().getDeclaredMethod("appendInsertPreapreStatement", parameterTypes);
    }

    /**
     * Sets up PostgreSQL update data for testing purposes.
     *
     * @param inputJson The JSON input string containing the update data
     * @throws SQLException If a database access error occurs
     */
    private void setUpPostGresUpdateData(String inputJson) throws SQLException {
        createPostgresStringTypeTestConfig();
        DataTypesUtil.setupInput(inputJson, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetString(_resultSetMetaExtractor);
        Mockito.when(_resultSetMetaExtractor.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("field");
    }

    /**
     * Sets up common test data for string data type operations in Postgres database tests.
     *
     * @return Map<String, String> A map containing field name to data type mapping
     * @throws SQLException If there is an error accessing the database metadata
     */
    private void createPostgresStringTypeTestConfig() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        Set<String> columnNames = new LinkedHashSet<>(Arrays.asList("field"));
        postgresUpsert = new PostgresUpsert(_connection, 1L, OBJECT_TYPE_ID,
                DatabaseConnectorConstants.COMMIT_BY_PROFILE, columnNames);
    }
}

