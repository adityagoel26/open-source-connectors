// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.insert;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

/**
 * Test class for {@link DynamicInsertTransactionOperation}
 */
public class DynamicInsertTransactionOperationTest {

    public static final String                                 EXECUTION_ID             = "executionId";
    public static final String                                 TEST_JSON                =
            "{\"time\":\"10:59:59\",\"isValid\":false,\"phNo\":\"9990449935\",\"price\":\"100\",\"JSON\":\"100\"}";
    public static final String SCHEMA = "dbv2";
    private final       OperationContext                       _operationContext        =
            new DatabaseConnectorTestContext().getDatabaseContext();
    private final       PropertyMap                            _propertyMap             = Mockito.mock(
            PropertyMap.class);
    private final       Connection                             _connection              = Mockito.mock(
            Connection.class);
    private final       DatabaseMetaData                       _databaseMetaData        = Mockito.mock(
            DatabaseMetaData.class);
    private final       PreparedStatement                      _preparedStatement       = Mockito.mock(
            PreparedStatement.class);
    private final       List<ObjectType>                       _list                    = Mockito.mock(ArrayList.class);
    private final       ResultSet                              _resultSet               = Mockito.mock(ResultSet.class);
    private final       TransactionDatabaseConnectorConnection _trnscDbConnection       = Mockito.mock(
            TransactionDatabaseConnectorConnection.class);
    private final       ResultSet                              _resultSetInsert         = Mockito.mock(ResultSet.class);
    private final       ResultSetMetaData                      _resultSetMetaData       = Mockito.mock(
            ResultSetMetaData.class);
    private final       SimpleTrackedData                      _document                =
            ResponseUtil.createInputDocument(TEST_JSON);
    private final       SimpleOperationResponse                _simpleOperationResponse = ResponseUtil.getResponse(
            Collections.singleton(_document));
    private final ResultSet _tableResultSet = Mockito.mock(ResultSet.class);

    /**
     * Setup mocks for tests.
     * @throws SQLException
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_trnscDbConnection.getContext()).thenReturn(_operationContext);
    }

    /**
     * Test {@link StandardInsertOperation#getConnection()} method.
     */
    @Test
    public void testGetConnection() {
        DynamicInsertTransactionOperation dynamicInsertTransactionOperation = new DynamicInsertTransactionOperation(
                new TransactionDatabaseConnectorConnection(_operationContext));
        Assert.assertNotNull(dynamicInsertTransactionOperation.getConnection());
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
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)}
     */
    @Test
    public void testExecuteSizeLimitedUpdate() throws SQLException {
        DynamicInsertTransactionOperation dynamicInsertTransactionOperation;
        OperationStatus                   operationStatus;

        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(_databaseMetaData.getColumns(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(resultSet);
        when(resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_databaseMetaData.getTables(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(_tableResultSet);
        Mockito.when(_tableResultSet.next()).thenReturn(true);
        Mockito.when(_connection.getSchema()).thenReturn(SCHEMA);
        when(resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("name");
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        setupDataForExecuteInsertOperation();

        dynamicInsertTransactionOperation = new DynamicInsertTransactionOperation(_trnscDbConnection);
        dynamicInsertTransactionOperation.executeSizeLimitedUpdate(ResponseUtil.toRequest(Collections.singleton(
                _document)), _simpleOperationResponse);

        operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)}
     */
    @Test
    public void testExecuteSizeLimitedUpdateValidateMetadata() throws SQLException {
        DynamicInsertTransactionOperation dynamicInsertTransactionOperation;
        List<SimplePayloadMetadata>       simplePayloadMetadata;

        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        when(_databaseMetaData.getColumns(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(resultSet);
        Mockito.when(_databaseMetaData.getTables(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(_tableResultSet);
        Mockito.when(_tableResultSet.next()).thenReturn(true);
        Mockito.when(_connection.getSchema()).thenReturn(SCHEMA);
        when(resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        when(resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        when(resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("name");
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        setupDataForExecuteInsertOperation();

        dynamicInsertTransactionOperation = new DynamicInsertTransactionOperation(_trnscDbConnection);
        dynamicInsertTransactionOperation.executeSizeLimitedUpdate(ResponseUtil.toRequest(Collections.singleton(
                _document)), _simpleOperationResponse);

        simplePayloadMetadata = _simpleOperationResponse.getResults().get(0).getPayloadMetadatas();
        Map<String, String> trackedProps = simplePayloadMetadata.get(0).getTrackedProps();
        Assert.assertTrue("Assert Transaction Id", trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS, trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * Test {@link StandardInsertOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)}
     */
    @Test
    public void testExecuteSizeLimitedUpdateNullConnection() throws SQLException {
        DynamicInsertTransactionOperation dynamicInsertTransactionOperation;

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        setupDataForExecuteInsertOperation();
        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenCallRealMethod();

        dynamicInsertTransactionOperation = new DynamicInsertTransactionOperation(_trnscDbConnection);
        try {
            dynamicInsertTransactionOperation.executeSizeLimitedUpdate(ResponseUtil.toRequest(Collections.singleton(
                    _document)), _simpleOperationResponse);
        } catch (ConnectorException e) {
            Assert.assertTrue(e.getMessage().contains("There is no existing transaction for TransactionId("));
        }
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    private void setupDataForExecuteInsertOperation() throws SQLException {
        DataTypesUtil.setUpTransactionConnectionObject(_connection, _databaseMetaData, _preparedStatement, _trnscDbConnection);
        DataTypesUtil.setUpObjectForStandardOperation(_databaseMetaData, _preparedStatement, _resultSet, _propertyMap,
                _list, "EVENT");
        DataTypesUtil.setUpResultObjectData(_resultSet);
        DataTypesUtil.setUpObjectForResultSetOperation(_preparedStatement, _resultSetInsert, _resultSet,
                _resultSetMetaData, new int[1]);
    }

    /**
     * Tests the scenario when the table does not exist for the DynamicInsertTransactionOperation.
     *
     * @throws SQLException if an error occurs while interacting with the database
     */
    @Test
    public void testTableNotExists() throws SQLException {
        ResultSet resultSet = Mockito.mock(ResultSet.class);

        Mockito.when(_trnscDbConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_trnscDbConnection.createCacheKey(Mockito.anyString())).thenReturn(
                new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_databaseMetaData.getColumns(Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(resultSet);
        Mockito.when(_resultSet.next()).thenReturn(false);

        setupDataForExecuteInsertOperation();

        DynamicInsertTransactionOperation dynamicInsertTransactionOperation = new DynamicInsertTransactionOperation(
                _trnscDbConnection);
        dynamicInsertTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        Assert.assertEquals("The specified table \"TEST\" does not exist",
                _simpleOperationResponse.getResults().get(0).getMessage());
    }
}
