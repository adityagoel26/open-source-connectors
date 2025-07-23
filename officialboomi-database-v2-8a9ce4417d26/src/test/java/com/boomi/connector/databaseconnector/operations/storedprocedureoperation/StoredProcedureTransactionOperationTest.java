package com.boomi.connector.databaseconnector.operations.storedprocedureoperation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestContext;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test class for {@link StoredProcedureTransactionOperation}
 */
public class StoredProcedureTransactionOperationTest {

    public static final String EXECUTION_ID = "executionId";
    public static final String TEST_JSON =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    private static final String PROCEDURE = "Test.TestProcedure";
    private static final String INPUT_DATA_FOR_SP = "{\"FirstName\" : \"ABC\",\"LastName\" : \"XYZ\" }";
    private static final String INPUT_FIRST_NAME = "FirstName";
    private static final String INPUT_LAST_NAME = "LastName";
    private static final String INPUT_USER_ID = "UserId";
    private static final long NEGATIVE_BATCH_COUNT = -3;
    private final OperationContext _operationContext = new DatabaseConnectorTestContext().getDatabaseContext();
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final List<ObjectType> _list = Mockito.mock(ArrayList.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final TransactionDatabaseConnectorConnection _trnscDbConnection = Mockito.mock(
            TransactionDatabaseConnectorConnection.class);
    private final ResultSet _resultSetInsert = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(TEST_JSON);
    private final SimpleOperationResponse _simpleOperationResponse = ResponseUtil.getResponse(
            Collections.singleton(_document));
    private final CallableStatement _callableStatement = Mockito.mock(CallableStatement.class);
    private final byte[] _byteArray = new byte[1];

    /**
     * Setup mocks for tests.
     */
    @Before
    public void setup() {
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext);
    }

    /**
     * Test for get connection
     */
    @Test
    public void testGetConnection() {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation =
                new StoredProcedureTransactionOperation(new TransactionDatabaseConnectorConnection(_operationContext));
        Assert.assertNotNull(storedProcedureTransactionOperation.getConnection());
    }

    /**
     * Test Constructor
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(
                new StoredProcedureTransactionOperation(new TransactionDatabaseConnectorConnection(_operationContext)));
    }

    /**
     * Test for ExecuteSizeLimitedUpdate() for success StoredProcedureTransactionOperation
     * @throws SQLException
     */
    @Test
        public void testExecuteSizeLimitedUpdate() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        OperationStatus operationStatus;

        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test to Validate Metadata for StoredProcedureTransactionOperation
     * @throws SQLException
     */
    @Test
    public void testExecuteSizeLimitedUpdateValidateMetadata() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        List<SimplePayloadMetadata>       simplePayloadMetadata;

        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(_databaseMetaData.getColumns(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(resultSet);
        Mockito.when(resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON);
        Mockito.when(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("name");
        Mockito.when(_databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);
        Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
        setupDataForStoredProcedureTransactionOperation();
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        simplePayloadMetadata = _simpleOperationResponse.getResults().get(0).getPayloadMetadatas();

        Map<String, String> trackedProps = simplePayloadMetadata.get(0).getTrackedProps();
        Assert.assertTrue("Assert Transaction Id",
                trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test for ExecuteSizeLimitedUpdate() for success StoredProcedureTransactionOperation validate payload metadata IN and OUT params
     * @throws SQLException
     */
    @Test
    public void testExecuteSizeLimitedUpdateINOUTPARAMS() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        List<SimplePayloadMetadata>       simplePayloadMetadata;

        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(ResponseUtil.toRequest(Collections.singleton(
                _document)), _simpleOperationResponse);
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);
        simplePayloadMetadata = _simpleOperationResponse.getResults().get(0).getPayloadMetadatas();
        Map<String, String> trackedProps = simplePayloadMetadata.get(0).getTrackedProps();

        Assert.assertTrue("Assert Transaction Id",
                trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
        Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                simpleOperationResult.getMessage());
        Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                simpleOperationResult.getStatusCode());
        Assert.assertEquals("Payload assert failed", "{\"FirstName\":\"\",\"LastName\":\"\",\"UserId\":\"\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test for ExecuteSizeLimitedUpdate() for success StoredProcedureTransactionOperation validate payload metadata NO params
     * @throws SQLException
     */
    @Test
    public void testExecuteSizeLimitedUpdateNOPARAMS() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        List<SimplePayloadMetadata> simplePayloadMetadata;

        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);
        Mockito.when(_callableStatement.execute()).thenReturn(true);
        Mockito.when(_callableStatement.getResultSet()).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(false).thenReturn(false).thenReturn(false).thenReturn(false)
                .thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(3);
        Mockito.when(_resultSetMetaData.getColumnType(1)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(2)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnType(3)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnLabel(1)).thenReturn("FirstName");
        Mockito.when(_resultSetMetaData.getColumnLabel(2)).thenReturn("LastName");
        Mockito.when(_resultSetMetaData.getColumnLabel(3)).thenReturn("UserId");
        Mockito.when(_resultSet.getString("FirstName")).thenReturn("");
        Mockito.when(_resultSet.getString("LastName")).thenReturn("");
        Mockito.when(_resultSet.getString("UserId")).thenReturn("");

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);
        simplePayloadMetadata = _simpleOperationResponse.getResults().get(0).getPayloadMetadatas();
        Map<String, String> trackedProps = simplePayloadMetadata.get(0).getTrackedProps();

        Assert.assertTrue("Assert Transaction Id",
                trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
        Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                simpleOperationResult.getMessage());
        Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                simpleOperationResult.getStatusCode());
        Assert.assertEquals("Payload assert failed", "{\"FirstName\":\"\",\"LastName\":\"\",\"UserId\":\"\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test for ExecuteSizeLimitedUpdate() for StoredProcedureTransactionOperation validate payload metadata
     * @throws SQLException
     */
    @Test
    public void testExecuteWithoutORACLEDataBaseTwo() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        List<SimplePayloadMetadata> simplePayloadMetadata;

        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);
        Mockito.when(_resultSet.getString(6)).thenReturn("-4");
        Mockito.when(_callableStatement.getString(Mockito.anyString())).thenReturn("outParams");
        Mockito.when(_resultSet.getInt(Mockito.anyInt())).thenReturn(-4);
        Mockito.when(_callableStatement.getBytes(Mockito.anyInt())).thenReturn(_byteArray);
        Mockito.when(_resultSet.next()).thenReturn(false);

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);
        simplePayloadMetadata = _simpleOperationResponse.getResults().get(0).getPayloadMetadatas();
        Map<String, String> trackedProps = simplePayloadMetadata.get(0).getTrackedProps();

        Assert.assertTrue("Assert Transaction Id",
                trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
        Assert.assertEquals("Message assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                simpleOperationResult.getMessage());
        Assert.assertEquals("Status code assert failed", DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                simpleOperationResult.getStatusCode());
        Assert.assertEquals("Payload assert failed",
                "{\"Status Code\":200,\"Message\":\"Procedure Executed Successfully!!\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test for Null Connection in StoredProcedureTransactionOperation
     * @throws SQLException
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullConnection() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        setupDataForStoredProcedureTransactionOperation();
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenCallRealMethod();

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        try {
            storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                    ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
            Assert.fail("Should have thrown exception");
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId("));
        }
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test for SQL Null Connection in StoredProcedureTransactionOperation
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullSqlConnection() {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        OperationStatus operationStatus;

        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(null);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, operationStatus);
    }

    /**
     * Test for MSSQLserver dqtabase in StoredProcedureTransactionOperation
     * @throws SQLException
     */
    @Test
    public void testExecuteSizeLimitedUpdateMSSQL() throws SQLException {
        StoredProcedureTransactionOperation storedProcedureTransactionOperation;
        OperationStatus operationStatus;

        setupDataForExecuteStoredProcedureOperation(INPUT_DATA_FOR_SP);
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQLSERVER);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_connection.prepareCall(Mockito.anyString())).thenReturn(_callableStatement);
        OperationContext _operationContext1 = Mockito.spy(_operationContext);
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext1);
        Mockito.when(_operationContext1.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getLongProperty(Mockito.any())).thenReturn(0L);

        storedProcedureTransactionOperation = new StoredProcedureTransactionOperation(_trnscDbConnection);
        storedProcedureTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);

        operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    private void setupDataForStoredProcedureTransactionOperation() throws SQLException {
        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement,
                _trnscDbConnection);
        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap,
                _list, "EVENT");
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet,
                _resultSetMetaData, new int[1]);
    }

    private void setupDataForExecuteStoredProcedureOperation(String input) throws SQLException {

        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement,
                _trnscDbConnection);
        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap,
                _list, "EVENT");
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet,
                _resultSetMetaData, new int[1]);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(
                        _databaseMetaData.getProcedureColumns(Mockito.any(), Mockito.any(), Mockito.anyString(),
                                Mockito.any()))
                .thenReturn(_resultSet);
        //      Mocking for Partial Result
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
    }
}
