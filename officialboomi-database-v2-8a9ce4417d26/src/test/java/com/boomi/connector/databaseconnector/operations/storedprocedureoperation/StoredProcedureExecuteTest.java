// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.storedprocedureoperation;

import oracle.jdbc.OracleType;
import oracle.sql.json.OracleJsonFactory;
import oracle.sql.json.OracleJsonObject;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.BrowserConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Class to test {@link StoredProcedureExecute}
 */
public class StoredProcedureExecuteTest {

    private static final String INPUT_DATA_FOR_SP = "{\"FirstName\" : \"ABC\",\"LastName\" : \"XYZ\" }";
    private static final String INPUT_DATA_FOR_SP_NESTED_JSON =
            "{\"EMP_DETAILS\":{\"ROLL_NO\":2,\"NAME\":\"John\",\"AGE\":25,\"CITY\":\"London\"}}";
    private static final String INPUT_TWO =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\"}";
    private static final String INPUT = "{\"name\":\"Test\",\"id\":2,\"date\":\"2023-03-29\"}";
    String INPUT_DATA =
            "{\"SID\":30,\"Semester\":\"6th Sem\",\"Marks\":85.5,\"EnrolledDate\":\"2023-03-29\",\"name\":\"John\","
                    + "\"Subject\":\"Biology\",\"duration\":36,\"fee\":100000.00,\"isQualified\":false,"
                    + "\"time\":\"10:59:59\","
                    + "\"misc\":null,\"Avg\":\"63.76F\",\"dateAndTime\":\"2018-09-01 09:01:15\"}";
    private static final String INPUT_JSON = "{\"u_data\" : null}";
    private static final String INPUT_DATA_NULL = "null";
    private static final String SCHEMA_NAME = "SchemaName";
    private static final String INPUT_FIRST_NAME = "FirstName";
    private static final String INPUT_LAST_NAME = "LastName";
    private static final String INPUT_USER_ID = "UserId";

    private static final int[] EXECUTE_BATCH_COUNT = new int[1];
    private static final String PROCEDURE = "Test.TestProcedure";
    private static final String ERROR_MESSAGE = "Response status is not SUCCESS";
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final CallableStatement _callableStatement = Mockito.mock(CallableStatement.class);
    private final Iterator<ObjectData> _objectDataIterator = Mockito.mock(Iterator.class);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final BrowserConnection browserConnection = new BrowserConnection();
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final Blob blob = Mockito.mock(Blob.class);
    private final byte[] byteArray = new byte[1];

    @Rule
    public ExpectedException exception = ExpectedException.none();
    private Method method;
    private Class[] parameterTypes = new Class[2];
    private Object[] parameters = new Object[2];

    @Before
    public void setup() throws SQLException {

        Mockito.when(_updateRequest.iterator()).thenReturn(_objectDataIterator);
        Mockito.when(_objectDataIterator.hasNext()).thenReturn(true, false);
        Mockito.when(_resultSet.getString("TYPE_NAME")).thenReturn("JSON");
        parameterTypes[0] = CallableStatement.class;
        parameterTypes[1] = InputStream.class;
    }

    @Test
    public void testExecuteMssqlServerCreateOperation() throws SQLException, IOException {

        String mssqlStoredProcedureInput = "{\"P_CUSTOMER_EIN\" : \"2\",\r\n" + "\"P_EMAIL_SUB\": \"a\"}";

        setupDataForWithoutOutParams(mssqlStoredProcedureInput);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(1L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecutePackagedCreateOperation() throws SQLException, IOException {

        String inputForPackagedSP = "{\"P_CUSTOMER_EIN_Category\" : \"2\",\r\n" + "\"P_EMAIL_CATEGORY\" : \"aaa\",\r\n"
                + "\"P_EMAIL_SENT_DT\" : \"aa\",\r\n" + "\"P1_EMAIL_SUB\": \"a\"}";

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        setupDataForWithoutOutParams(inputForPackagedSP);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(1L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteStoredProcedureNegativeBatchCount() throws SQLException, IOException {

        String expectedErrorMessageForNegativeBatchCount = DatabaseConnectorConstants.BATCH_COUNT_CANNOT_BE_NEGATIVE;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        exception.expect(ConnectorException.class);
        exception.expectMessage(expectedErrorMessageForNegativeBatchCount);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(-1L, 0L, 0L, 0);
    }

    @Test
    public void testExecuteStoredProcedureInParamEmpty() throws SQLException, IOException {

        String expectedErrorMessageForInParamEmpty = "Batching cannot be applied for non input parameter procedures";
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        exception.expect(ConnectorException.class);
        exception.expectMessage(expectedErrorMessageForInParamEmpty);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(1L, 0L, 0L, 0);
    }

    @Test
    public void testExecuteStoredProcedureOperation() throws SQLException, IOException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteStoredProcedureOperationForOracle() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(6)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn("Test");

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, "TestSchemaTwo");
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test case for executeStatements method when the database is PostgreSQL.
     * This test verifies that the method correctly handles PostgreSQL-specific behavior with refCursor as out parameter
     *
     *@throws SQLException the SQL exception
     *@throws IOException  Signals that an I/O exception has occurred.
     */
    @Test
    public void testExecuteStoredProcedureForPOSTGRESQL_RefCursor() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(6)).thenReturn("2012");
        Mockito.when(_resultSet.getString(4)).thenReturn(INPUT_FIRST_NAME)
                .thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_FIRST_NAME);
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"))
                .thenReturn(Short.valueOf("2")).thenReturn(Short.valueOf("2")).thenReturn(Short.valueOf("2"))
                .thenReturn(Short.valueOf("2")).thenReturn(Short.valueOf("0"));

        Mockito.when(_callableStatement.getObject(Mockito.anyInt())).thenReturn(_resultSet);
        Mockito.when(_callableStatement.execute()).thenReturn(true);
        Mockito.when(_callableStatement.getResultSet()).thenReturn(_resultSet);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(3);
        Mockito.when(_resultSetMetaData.getColumnType(1)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(2)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(3)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnName(1)).thenReturn("FirstName");
        Mockito.when(_resultSetMetaData.getColumnName(2)).thenReturn("LastName");
        Mockito.when(_resultSetMetaData.getColumnName(3)).thenReturn("UserId");
        Mockito.when(_resultSet.getString("FirstName")).thenReturn("ABC");
        Mockito.when(_resultSet.getString("LastName")).thenReturn("XYZ");
        Mockito.when(_resultSet.getString("UserId")).thenReturn("123");

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);

        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Mockito.verify(_connection, Mockito.times(1)).setAutoCommit(false);
        Mockito.verify(_callableStatement, Mockito.times(1))
                .registerOutParameter(1, Types.OTHER);
        Mockito.verify(_connection, Mockito.times(1)).commit();

        Assert.assertEquals(ERROR_MESSAGE,OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals("200", simpleOperationResponse.getResults().get(0).getStatusCode());
    }

    /**
     * Test case for executeStatements method when the database is PostgreSQL without RefCursor.
     * This test verifies that the method correctly handles PostgreSQL-specific behavior when the out parameter
     * is not a RefCursor
     *
     * @throws SQLException the SQL exception
     * @throws IOException  Signals that an I/O exception has occurred.
     */
    @Test
    public void testExecuteStoredProcedureForPOSTGRESQLWithoutRefCursor() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(6)).thenReturn("12");
        Mockito.when(_callableStatement.getObject(Mockito.anyInt())).thenReturn(_resultSet);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals("200", simpleOperationResponse.getResults().get(0).getStatusCode());
    }

    @Test
    public void testExecuteCommitByRowsInsertBatchTwo() throws SQLException, JsonProcessingException {

        String mssqlStoredProcedureInput = "{\"P1_CUSTOMER_EIN\" : \"2\",\r\n" + "\"P2_EMAIL_SUB\": \"a\"}";

        setupDataForWithoutOutParams(mssqlStoredProcedureInput);
        DataTypesUtil.setupInput(INPUT_TWO, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(
                DatabaseConnectorConstants.COMMIT_BY_ROWS);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(2L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByRowsDateException() throws SQLException, JsonProcessingException {

        String mssqlStoredProcedureInput = "{\"P2_CUSTOMER_EIN\" : \"2\",\r\n" + "\"P3_EMAIL_SUB\": \"a\"}";
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(
                DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);

        SimpleTrackedData data1 = new SimpleTrackedData(13,
                new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        SimpleTrackedData data = new SimpleTrackedData(2,
                new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.seUpTrackedDataList(_updateRequest, simpleOperationResponse, Arrays.asList(data1, data));
        DataTypesUtil.setUpForDateException(_resultSet);
        setupDataForWithoutOutParams(mssqlStoredProcedureInput);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(2L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests executing a stored procedure for an Oracle database setup.
     * Verifies that the operation status is SUCCESS after execution.
     */
    @Test
    public void testExecuteForOracleDataBase() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("JSON");
        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(1111);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn(null);

        Mockito.when(_callableStatement.getObject(Mockito.anyInt())).thenReturn(blob);
        Mockito.when(_callableStatement.getBlob(Mockito.anyInt())).thenReturn(blob);
        Mockito.when(blob.getBytes(Mockito.anyLong(), Mockito.anyInt())).thenReturn(byteArray);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests executing a stored procedure with OUT parameters but without BLOBs.
     * Ensures the operation completes successfully with a SUCCESS status.
     */
    @Test
    public void testExecuteForOutParamsWithoutBlob() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("JSON");

        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(1111);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn(null);
        Mockito.when(_callableStatement.getObject(Mockito.anyInt())).thenReturn(67);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteWithoutORACLEDataBase() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        Mockito.when(_callableStatement.getString(Mockito.anyString())).thenReturn("testOut");
        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(4);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteWithoutORACLEDataBaseForNegativeValue() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(6)).thenReturn("-4");

        Mockito.when(_callableStatement.getString(Mockito.anyString())).thenReturn("testOutParams");
        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(-4);
        Mockito.when(_callableStatement.getBytes(Mockito.anyInt())).thenReturn(byteArray);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteWithoutORACLEDataBaseTwo() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_resultSet.getString(6)).thenReturn("-4");
        Mockito.when(_callableStatement.getString(Mockito.anyString())).thenReturn("outParams");

        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(-4);
        Mockito.when(_callableStatement.getBytes(Mockito.anyInt())).thenReturn(byteArray);
        Mockito.when(_resultSet.next()).thenReturn(false);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteStoredProcedureOperationWithParameterTypeNVCHAR() throws SQLException, IOException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        setupDataForExecuteStoredProcedureOperationWithParameterTypeNVCHAR(INPUT_DATA_FOR_SP_NESTED_JSON);
        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    private void setupDataForExecuteStoredProcedureOperation(String input) throws SQLException {

        InputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(123, inputStream);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        Mockito.when(_callableStatement.executeBatch()).thenReturn(EXECUTE_BATCH_COUNT);

        Mockito.when(
                        _databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);

        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(
                true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(
                false).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(
                true).thenReturn(true).thenReturn(false);

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TABLE)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME)).thenReturn("PROCEDURE_NAME");

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(INPUT_FIRST_NAME)
                .thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(
                        INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_USER_ID).thenReturn(INPUT_USER_ID)
                .thenReturn(INPUT_USER_ID).thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_FIRST_NAME).thenReturn(
                        INPUT_FIRST_NAME).thenReturn(INPUT_USER_ID).thenReturn(INPUT_USER_ID).thenReturn(INPUT_USER_ID)
                .thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME);

        Mockito.when(_resultSet.getString(4)).thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(
                INPUT_FIRST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_USER_ID);

        Mockito.when(_resultSet.getString(INPUT_USER_ID)).thenReturn("12");
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"));

        Mockito.when(_resultSet.getString(6)).thenReturn("12");
        Mockito.when(_callableStatement.getString(1)).thenReturn(INPUT_FIRST_NAME);
        Mockito.when(_callableStatement.getString(2)).thenReturn(INPUT_LAST_NAME);
        Mockito.when(_callableStatement.getString(3)).thenReturn(INPUT_USER_ID);
    }

    private void setupDataForWithoutOutParams(String input) throws SQLException {

        InputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(123, inputStream);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        Mockito.when(_callableStatement.executeBatch()).thenReturn(EXECUTE_BATCH_COUNT);

        Mockito.when(
                        _databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);

        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(
                true).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(false).thenReturn(true).thenReturn(
                true).thenReturn(true).thenReturn(false);

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TABLE)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("JSON");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME)).thenReturn("PROCEDURE_NAME");

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(INPUT_FIRST_NAME)
                .thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(
                        INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_USER_ID).thenReturn(INPUT_USER_ID)
                .thenReturn(INPUT_USER_ID).thenReturn(INPUT_FIRST_NAME).thenReturn(INPUT_FIRST_NAME).thenReturn(
                        INPUT_FIRST_NAME).thenReturn(INPUT_USER_ID).thenReturn(INPUT_USER_ID).thenReturn(INPUT_LAST_NAME)
                .thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME).thenReturn(INPUT_LAST_NAME);

        Mockito.when(_resultSet.getString(INPUT_USER_ID)).thenReturn("12");
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"));

        Mockito.when(_resultSet.getString(6)).thenReturn("12");
    }

    private void setupDataForExecuteStoredProcedureOperationWithParameterTypeNVCHAR(String input) throws SQLException {

        InputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(123, inputStream);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        Mockito.when(_callableStatement.executeBatch()).thenReturn(EXECUTE_BATCH_COUNT);
        Mockito.when(
                        _databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false)
                .thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TABLE)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_NAME)).thenReturn("PROCEDURE_NAME");

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("EMP_DETAILS").thenReturn(
                "EMP_DETAILS").thenReturn("EMP_DETAILS");
        Mockito.when(_resultSet.getString(4)).thenReturn("EMP_DETAILS").thenReturn("EMP_DETAILS").thenReturn(
                "EMP_DETAILS");

        Mockito.when(_resultSet.getString("EMP_DETAILS")).thenReturn("-9");
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf("2"));
        Mockito.when(_resultSet.getString(6)).thenReturn("-9");
    }

    @Test
    public void testExecuteLongNVarcharDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "Semester";
        String dataTypeValue = "12";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setString(columnName, "6th Sem");
    }

    @Test
    public void testExecuteLongVarcharDataTypeWithJsonType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "name";
        String dataTypeValue = "-1";

        Mockito.when(_resultSet.getString("TYPE_NAME")).thenReturn("JSON");

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setString(columnName, "John");
    }

    @Test
    public void testExecuteIntegerDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "SID";
        String dataTypeValue = "4";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setInt(columnName, 30);
    }

    @Test
    public void testExecuteDateDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "EnrolledDate";
        String dataTypeValue = "91";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setString(columnName, "2023-03-29");
    }

    @Test
    public void testExecuteTimeDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "time";
        String dataTypeValue = "92";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setTime(columnName, Time.valueOf("10:59:59"));
    }

    @Test
    public void testExecuteBooleanDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "isQualified";
        String dataTypeValue = "16";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setBoolean(columnName, false);
    }

    @Test
    public void testExecuteBigIntDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "duration";
        String dataTypeValue = "-5";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setLong(columnName, 36);
    }

    @Test
    public void testExecuteDoubleDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "Marks";
        String dataTypeValue = "8";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setDouble(columnName, 85.5);
    }

    @Test
    public void testExecuteDecimalDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        String columnName = "fee";
        String dataTypeValue = "3";
        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setBigDecimal(columnName, BigDecimal.valueOf(100000.00));
    }

    @Test
    public void testExecuteRealDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "Avg";
        String dataTypeValue = "7";
        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setFloat(columnName, 63.76F);
    }

    @Test
    public void testExecuteTimeStampDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "dateAndTime";
        String dataTypeValue = "93";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setTimestamp(columnName,
                Timestamp.valueOf("2018-09-01 09:01:15"));
    }

    @Test
    public void testExecuteOtherDataType()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "EMP_DETAILS";
        String dataTypeValue = "1111";
        OracleJsonFactory factory = new OracleJsonFactory();
        OracleJsonObject object = factory.createObject();

        object.put("ROLL_NO", "2");
        object.put("NAME", "John");
        object.put("AGE", "25");
        object.put("CITY", "London");

        Mockito.when(_resultSet.getString("TYPE_NAME")).thenReturn("JSON");

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA_FOR_SP_NESTED_JSON);

        Mockito.verify(_callableStatement, Mockito.times(1)).setObject(columnName, object, OracleType.JSON);
    }

    @Test
    public void testExecuteInputDataWithNull()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "misc";
        String dataTypeValue = "4";

        setupDataForDataTypes(columnName, dataTypeValue, INPUT_DATA);
        Mockito.verify(_callableStatement, Mockito.times(1)).setNull(columnName, Types.VARCHAR);
    }

    /**
     * This method will set data for data types
     * statement based on the incoming requests.
     *
     * @param columnName    the column name
     * @param dataTypeValue the data type value
     * @param inputData     the input data
     * @throws SQLException              the SQL exception
     * @throws NoSuchMethodException     the No Such Method Exception
     * @throws IllegalAccessException    illegal access  exception
     * @throws InvocationTargetException invocation target exception.
     */
    private void setupDataForDataTypes(String columnName, String dataTypeValue, String inputData)
            throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        String dataTypeValueForShortType = "2";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        setupMockDataForDataTypes(inputStream);
        setDataForOnColumnNameAndDataType(columnName, dataTypeValueForShortType, dataTypeValue);
        invokePrivateStaticMethod(inputStream);
    }

    /**
     * This method will invoke the private method
     * statement based on the incoming requests.
     *
     * @param inputStream the input stream
     * @throws SQLException              the SQL exception
     * @throws NoSuchMethodException     the No Such Method Exception
     * @throws IllegalAccessException    illegal access  exception
     * @throws InvocationTargetException invocation target exception.
     */
    private void invokePrivateStaticMethod(InputStream inputStream)
            throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);

        parameters[0] = _callableStatement;
        parameters[1] = inputStream;

        method = execute.getClass().getDeclaredMethod("prepareStatements", parameterTypes);
        method.setAccessible(true);
        method.invoke(execute, parameters);
    }

    /**
     * This method will set up mock data based input data
     * statement based on the incoming requests.
     *
     * @param inputStream the input stream
     * @throws SQLException the SQL exception
     */
    private void setupMockDataForDataTypes(InputStream inputStream) throws SQLException {

        SimpleTrackedData trackedData = new SimpleTrackedData(123, inputStream);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        Mockito.when(
                        _databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);

        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false)
                .thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(
                        false);
    }

    /**
     * This method will set up mock data based input data
     * statement based on the incoming requests.
     *
     * @param columnName                the column name
     * @param dataTypeValueForShortType the data type value for short type
     * @param dataTypeValue             the data type value
     * @throws SQLException the SQL exception
     */
    private void setDataForOnColumnNameAndDataType(String columnName, String dataTypeValueForShortType,
            String dataTypeValue) throws SQLException {
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(columnName);
        Mockito.when(_resultSet.getString(4)).thenReturn(columnName);
        Mockito.when(_resultSet.getShort(5)).thenReturn(Short.valueOf(dataTypeValueForShortType));
        Mockito.when(_resultSet.getString(6)).thenReturn(dataTypeValue);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn("Test");
    }

    /**
     * Test Unique constraint exception
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws SQLException, IOException {

        String inputForPackagedSP = "{\"P_CUSTOMER_EIN_Category\" : \"2\",\r\n" + "\"P_EMAIL_CATEGORY\" : \"aaa\",\r\n"
                + "\"P_EMAIL_SENT_DT\" : \"aa\",\r\n" + "\"P1_EMAIL_SUB\": \"a\"}";

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForWithoutOutParams(inputForPackagedSP);
        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE,
                new int[] { 1, 2 });
        Mockito.when(_callableStatement.executeBatch()).thenThrow(bac);
        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(1L, 0L, 0L, 0);

        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE,
                simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test case to validate the execution of input data with multiple null values for PostgreSQL stored procedures.
     *
     * @throws SQLException
     * @throws InvocationTargetException
     * @throws IllegalAccessException
     * @throws NoSuchMethodException
     */
    @Test
    public void testExecuteInputWithDataTypeNullForPOSTGRESQL()
            throws SQLException, InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        String columnName = "u_data";
        String dataTypeValue = "4";
        setupDataForDataTypesForPOSTGRESQL(columnName, dataTypeValue, INPUT_JSON);
        Mockito.verify(_callableStatement, Mockito.times(1)).setNull(1, Types.NULL);
    }

    /**
     * This test method will set null as Database NULL for POSTGRESQL.
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteInputDataWithNullForPOSTGRESQL() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn("PostgreSQL");
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_NULL);
        Mockito.when(_resultSet.getString(6)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.PROCEDURE_PACKAGE_NAME)).thenReturn("Test");

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, "TestSchemaTwo");
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test to check if payload and result is correct.
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteStoredProcedureOperationPayloadSuccess() throws SQLException, IOException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        SimpleOperationResult simpleOperationResult = simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
        Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                simpleOperationResult.getMessage());
        Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                simpleOperationResult.getStatusCode());
        Assert.assertTrue("Payload metadata assert failed", simpleOperationResult.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"FirstName\":\"FirstName\",\"LastName\":\"LastName\",\"UserId\":\"UserId\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is correct for ProcessResultset with No params
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteStoredProcedureOperationPayloadSuccessProcessResultset() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_callableStatement.execute()).thenReturn(true);
        Mockito.when(_callableStatement.getResultSet()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(3);
        Mockito.when(_resultSetMetaData.getColumnType(1)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(2)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(3)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnLabel(1)).thenReturn("FirstName");
        Mockito.when(_resultSetMetaData.getColumnLabel(2)).thenReturn("LastName");
        Mockito.when(_resultSetMetaData.getColumnLabel(3)).thenReturn("UserId");
        Mockito.when(_resultSet.getString("FirstName")).thenReturn("FirstName");
        Mockito.when(_resultSet.getString("LastName")).thenReturn("LastName");
        Mockito.when(_resultSet.getString("UserId")).thenReturn("UserId");

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        SimpleOperationResult simpleOperationResult = simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
        Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                simpleOperationResult.getMessage());
        Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                simpleOperationResult.getStatusCode());
        Assert.assertTrue("Payload metadata assert failed", simpleOperationResult.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"FirstName\":\"FirstName\",\"LastName\":\"LastName\",\"UserId\":\"UserId\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is correct for multiple docs
     *
     * @throws SQLException
     * @throws JsonProcessingException
     */
    @Test
    public void testExecuteStoredProcedureOperationPayloadSuccessMultiDocs()
            throws SQLException, JsonProcessingException {
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT_TWO.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        Assert.assertEquals("Assert that there are 2 docs", 2, results.size());

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
            Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                    simpleOperationResult.getMessage());
            Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                    simpleOperationResult.getStatusCode());
            Assert.assertTrue("Payload metadata assert failed", simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"FirstName\":\"FirstName\",\"LastName\":\"LastName\",\"UserId\":\"UserId\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Sets up data types for  POSTGRESQL stored procedures.
     *
     * @param columnName
     * @param dataTypeValue
     * @param inputData
     * @throws SQLException
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private void setupDataForDataTypesForPOSTGRESQL(String columnName, String dataTypeValue, String inputData)
            throws SQLException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        String dataTypeValueForShortType = "2";
        InputStream inputStream = new ByteArrayInputStream(inputData.getBytes(StandardCharsets.UTF_8));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setupMockDataForDataTypes(inputStream);
        setDataForOnColumnNameAndDataType(columnName, dataTypeValueForShortType, dataTypeValue);
        invokePrivateStaticMethod(inputStream);
    }

    /**
     * Tests executeStatements for Oracle DB with RefCursor output.
     * Verifies successful execution and correct operation status.
     */
    @Test
    public void testExecuteForOracleDataBaseWithRefCursor() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        Mockito.when(_callableStatement.getObject(Mockito.anyString())).thenReturn(_resultSet);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.JSON).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.JSON).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.REF_CURSOR);

        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(8);
        int[] columnTypes = {
                Types.INTEGER, Types.NUMERIC, Types.VARCHAR, Types.REAL, Types.DOUBLE, Types.BOOLEAN,
                Types.CLOB,Types.OTHER };

        Mockito.when(_resultSetMetaData.getColumnType(Mockito.anyInt())).thenAnswer(invocation -> {
            int index = invocation.getArgument(0);
            return columnTypes[index - 1];
        });
        Mockito.when(_resultSetMetaData.getColumnName(Mockito.anyInt())).thenReturn("FirstName");
        Mockito.when(_resultSet.getString("FirstName")).thenReturn("FirstName");
        Mockito.when(_resultSet.getObject("FirstName")).thenReturn("FirstName");
        Mockito.when(_resultSet.getClob("FirstName")).thenReturn(Mockito.mock(Clob.class));

        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests executeStatements for Oracle DB when RefCursor is in an unexpected position.
     * Verifies that the method throws an exception as expected.
     */
    @Test
    public void testExecuteForOracleDataBaseWithRefCursorWithError() throws SQLException, JsonProcessingException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);

        Mockito.when(_callableStatement.getObject(Mockito.anyString())).thenReturn(_resultSet);
        Mockito.when(_resultSet.getMetaData()).thenThrow(new SQLException());
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.JSON).thenReturn(
                DatabaseConnectorConstants.REF_CURSOR).thenReturn(DatabaseConnectorConstants.JSON).thenReturn(
                DatabaseConnectorConstants.JSON).thenReturn(DatabaseConnectorConstants.REF_CURSOR);

        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenThrow(new SQLException("No Meta Data Found"));
        StoredProcedureExecute execute = new StoredProcedureExecute(_connection, PROCEDURE, _updateRequest,
                simpleOperationResponse, browserConnection, SCHEMA_NAME);
        execute.executeStatements(0L, 0L, 0L, 0);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, operationStatus);

    }
}