// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.upsert;

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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test class for {@link UpsertTransactionOperation}
 */
public class UpsertTransactionOperationTest {

    public static final String EXECUTION_ID = "executionId";
    private static final String TEST_JSON =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final TransactionDatabaseConnectorConnection _trnscDbConnection =
            Mockito.mock(TransactionDatabaseConnectorConnection.class);
    private final ResultSet _resultSetInsert = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final SimpleTrackedData document = ResponseUtil.createInputDocument(TEST_JSON);
    private final SimpleOperationResponse simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(document));


    /**
     * Setup mocks for tests.
     *
     * @throws SQLException
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString()))
                .thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_connection.getCatalog()).thenReturn("testCatalog");
        Mockito.when(_databaseMetaData.getPrimaryKeys(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class))).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getIndexInfo(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class), Mockito.anyBoolean(), Mockito.anyBoolean())).thenReturn(_resultSet);
    }

    /**
     * Test {@link UpsertTransactionOperation#getConnection()} method.
     */
    @Test
    public void testGetConnection() {
        UpsertTransactionOperation UpsertTransactionOperation = new UpsertTransactionOperation(
                new TransactionDatabaseConnectorConnection(_operationContext));
        Assert.assertNotNull(UpsertTransactionOperation.getConnection());
    }

    /**
     * Test {@link UpsertTransactionOperation#UpsertTransactionOperation(TransactionDatabaseConnectorConnection)}
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(
                new UpsertTransactionOperation(new TransactionDatabaseConnectorConnection(_operationContext)));
    }

    /**
     * Test {@link UpsertTransactionOperation #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdate() throws SQLException {
        UpsertTransactionOperation upsertTransactionOperation;
        OperationStatus            operationStatus;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        setupDataForExecuteUpsertOperation();
        upsertTransactionOperation = new UpsertTransactionOperation(_trnscDbConnection);
        upsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link UpsertTransactionOperation #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateJoinTransactionMySQL() throws SQLException {
        UpsertTransactionOperation upsertTransactionOperation;
        OperationStatus            operationStatus;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        setupDataForExecuteUpsertOperation();
        upsertTransactionOperation = new UpsertTransactionOperation(_trnscDbConnection);
        upsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link UpsertTransactionOperation #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateJoinTransactionPostgre() throws SQLException {
        UpsertTransactionOperation upsertTransactionOperation;
        OperationStatus            operationStatus;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);

        setupDataForExecuteUpsertOperation();
        upsertTransactionOperation = new UpsertTransactionOperation(_trnscDbConnection);
        upsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link UpsertTransactionOperation #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullConnection() throws SQLException {
        UpsertTransactionOperation upsertTransactionOperation;
        SimpleOperationResult      simpleOperationResult;
        OperationStatus            operationStatus;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        setupDataForExecuteUpsertOperation();
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(null);
        upsertTransactionOperation = new UpsertTransactionOperation(_trnscDbConnection);
        upsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(document)), simpleOperationResponse);
        simpleOperationResult = simpleOperationResponse.getResults().get(0);
        operationStatus = simpleOperationResult.getStatus();
        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals("Assert message",
                DatabaseConnectorConstants.CONNECTION_FAILED_ERROR,
                simpleOperationResult.getMessage());
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link UpsertTransactionOperation #executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateValidateMetadata() throws SQLException {
        UpsertTransactionOperation upsertTransactionOperation;
        OperationStatus operationStatus;
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        setupDataForExecuteUpsertOperation();
        upsertTransactionOperation = new UpsertTransactionOperation(_trnscDbConnection);
        upsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        List<SimplePayloadMetadata> payloadMetadata = simpleOperationResponse.getResults().get(0)
                .getPayloadMetadatas();
        Map<String, String> trackedProps = payloadMetadata.get(0).getTrackedProps();
        Assert.assertTrue("Assert Transaction Id", trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    private void setupDataForExecuteUpsertOperation() throws SQLException {
        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement, _trnscDbConnection);
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet,
                _resultSetMetaData, new int[1]);
    }
}
