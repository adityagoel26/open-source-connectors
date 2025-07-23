// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.update;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This is a test class for the DynamicUpdateTransactionOperation.
 * It uses JUnit for testing and Mockito for mocking dependencies.
 */
public class DynamicUpdateTransactionOperationTest {

    // Declaring constants and necessary variables
    private static final String TEST = "TEST";
    private static final String STATUS = "STATUS";
    private static final String PO_ID = "PO_ID";
    private final static String TRANSACTION_STATUS_ERROR_MESSAGE = "Transaction status is not valid";
    private final static String TRANSACTION_ID_ERROR_MESSAGE = "Transaction id is not valid";
    private static final String INPUT =
            "{\"SET\": [{\"column\": \"STATUS\",\"value\": \"rejected\"}],\"WHERE\":[{\"column\": \"PO_ID\","
                    + "\"value\": 90,\"operator\": \"=\"}]}";
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
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(INPUT);
    private final SimpleOperationResponse _simpleOperationResponse = ResponseUtil.getResponse(
            Collections.singletonList(_document));
    private final TransactionCacheKey transactionCacheKey = new TransactionCacheKey(ResponseUtil.EXECUTION_ID,
            null);

    /**
     * This method sets up the necessary conditions for the tests.
     * It is annotated with @Before, so it runs before each test method.
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext);
    }

    /**
     * This test checks if the getConnection method returns the correct object.
     */
    @Test
    public void testGetConnection() {
        DynamicUpdateTransactionOperation dynamicUpdateTransactionOperation = new DynamicUpdateTransactionOperation(
                new TransactionDatabaseConnectorConnection(_operationContext));
        Assert.assertNotNull(dynamicUpdateTransactionOperation.getConnection());
    }

    /**
     * This test checks if the constructor creates the object correctly.
     */
    @Test
    public void testConstructor() {
        Assert.assertNotNull(
                new DynamicUpdateTransactionOperation(new TransactionDatabaseConnectorConnection(_operationContext)));
    }

    /**
     * This test checks if the executeSizeLimitedUpdate method executes the update operation when joinTransaction is
     * true.
     */
    @Test
    public void testExecuteSizeLimitedUpdateWhenJoinTransactionTrue() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(
                true).thenReturn(false);
        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement,
                _trnscDbConnection);
        setupDataForExecuteUpdateOperation();

        DynamicUpdateTransactionOperation dynamicUpdateTransactionOperation = new DynamicUpdateTransactionOperation(
                _trnscDbConnection);
        dynamicUpdateTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);

        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);
        OperationStatus operationStatus = simpleOperationResult.getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(TRANSACTION_STATUS_ERROR_MESSAGE, TransactionConstants.TRANSACTION_IN_PROGRESS,
                _simpleOperationResponse.getPayloadMetadatas().get(0).getTrackedProps()
                        .get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(TRANSACTION_ID_ERROR_MESSAGE, transactionCacheKey.toString(),
                _simpleOperationResponse.getPayloadMetadatas().get(0).getTrackedProps()
                        .get(TransactionConstants.TRANSACTION_ID));

        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * This test checks if the executeSizeLimitedUpdate method handles null connection correctly.
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullConnection() throws SQLException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);

        setupDataForExecuteUpdateOperation();

        DynamicUpdateTransactionOperation dynamicUpdateTransactionOperation = new DynamicUpdateTransactionOperation(
                _trnscDbConnection);
        dynamicUpdateTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);

        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);
        OperationStatus operationStatus = simpleOperationResult.getStatus();

        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals("Assert message", DatabaseConnectorConstants.CONNECTION_FAILED_ERROR,
                simpleOperationResult.getMessage());

        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * This method sets up the necessary data for the executeUpdateOperation tests.
     */
    private void setupDataForExecuteUpdateOperation() throws SQLException {

        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap,
                _list, TEST);

        Mockito.when(_trnscDbConnection.createCacheKey(ResponseUtil.EXECUTION_ID)).thenReturn(transactionCacheKey);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.STRING);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(STATUS).thenReturn(STATUS)
                .thenReturn(PO_ID).thenReturn(PO_ID).thenReturn(STATUS).thenReturn(PO_ID);

        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet,
                _resultSetMetaData, new int[1]);
    }
}