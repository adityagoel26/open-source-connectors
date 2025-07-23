// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Test class for {@link StandardInsertTransactionOperation}
 */
public class StandardInsertTransactionOperationTest {

    public static final String EXECUTION_ID = "executionId";
    public static final String TEST_JSON =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    private final Connection _connection = Mockito.mock(Connection.class);
    private final java.sql.DatabaseMetaData _databaseMetaData = Mockito.mock(java.sql.DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final TransactionDatabaseConnectorConnection _transDbConnection = Mockito.mock(
            TransactionDatabaseConnectorConnection.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final TransactionCacheKey _transactionCacheKey = Mockito.mock(TransactionCacheKey.class);


    /**
     * Setup mocks for tests.
     * @throws SQLException
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_transDbConnection.getContext()).thenReturn(_operationContext);
    }

    /**
     * Test {@link StandardInsertOperation#getConnection()} method.
     */
    @Test
    public void testGetConnection() {
        StandardInsertTransactionOperation standardInsertTransactionOperation = new StandardInsertTransactionOperation(
                new TransactionDatabaseConnectorConnection(_operationContext));
        Assert.assertNotNull(standardInsertTransactionOperation.getConnection());
    }

    /**
     * Test {@link StandardInsertOperation#StandardInsertOperation(DatabaseConnectorConnection)}
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(
                new StandardInsertTransactionOperation(new TransactionDatabaseConnectorConnection(_operationContext)));
    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdate() throws SQLException {
        StandardInsertTransactionOperation standardInsertTransactionOperation;
        OperationStatus operationStatus;
        Mockito.when(_transDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_transDbConnection.createCacheKey(Mockito.any())).thenReturn(_transactionCacheKey);
        setupDataForExecuteInsertOperation();
        SimpleTrackedData document = ResponseUtil.createInputDocument(TEST_JSON);
        SimpleOperationResponse simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(document));
        standardInsertTransactionOperation = new StandardInsertTransactionOperation(_transDbConnection);
        standardInsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullConnection() throws SQLException {
        StandardInsertTransactionOperation standardInsertTransactionOperation;
        SimpleOperationResult              simpleOperationResult;
        OperationStatus                    operationStatus;

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        setupDataForExecuteInsertOperation();
        Mockito.when(_transDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(null);
        Mockito.when(_transDbConnection.createCacheKey(Mockito.any())).thenReturn(_transactionCacheKey);
        SimpleTrackedData document = ResponseUtil.createInputDocument(TEST_JSON);
        SimpleOperationResponse simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(document));
        standardInsertTransactionOperation = new StandardInsertTransactionOperation(_transDbConnection);
        standardInsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), simpleOperationResponse);

        simpleOperationResult = simpleOperationResponse.getResults().get(0);
        operationStatus = simpleOperationResult.getStatus();
        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals("Assert message", simpleOperationResult.getMessage(),
                DatabaseConnectorConstants.CONNECTION_FAILED_ERROR);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateFailureScenario() throws SQLException {
        StandardInsertTransactionOperation standardInsertTransactionOperation;
        OperationStatus operationStatus;
        SimpleOperationResult simpleOperationResult;
        Mockito.when(_transDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_transDbConnection.createCacheKey(Mockito.any())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _operationContext.getOperationProperties()));
        setupDataForExecuteInsertOperation();
        SimpleTrackedData document = ResponseUtil.createInputDocument(TEST_JSON);
        SimpleOperationResponse simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(document));
        standardInsertTransactionOperation = new StandardInsertTransactionOperation(_transDbConnection);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        standardInsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), simpleOperationResponse);
        simpleOperationResult = simpleOperationResponse.getResults().get(0);
        operationStatus = simpleOperationResult.getStatus();
        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();

    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)} join
     * transaction true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateValidateMetadata() throws SQLException {
        StandardInsertTransactionOperation standardInsertTransactionOperation;
        OperationStatus operationStatus;
        Mockito.when(_transDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_transDbConnection.createCacheKey(Mockito.any())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _operationContext.getOperationProperties()));
        setupDataForExecuteInsertOperation();
        SimpleTrackedData document = ResponseUtil.createInputDocument(TEST_JSON);
        SimpleOperationResponse simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(document));
        standardInsertTransactionOperation = new StandardInsertTransactionOperation(_transDbConnection);
        standardInsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton((ObjectData) document)), simpleOperationResponse);
        operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        List<SimplePayloadMetadata> payloadMetadata = simpleOperationResponse.getResults().get(0)
                .getPayloadMetadatas();
        Map<String, String> trackedProps = payloadMetadata.get(0).getTrackedProps();
        Assert.assertTrue("Assert Transaction Id", trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS, trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    private void setupDataForExecuteInsertOperation() throws SQLException {
        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement, _transDbConnection);
        Mockito.when(_databaseMetaData.getColumns(null, null, "TEST", null)).thenReturn(_resultSet);
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSet, _resultSet,
                _resultSetMetaData, new int[1]);
    }
}
