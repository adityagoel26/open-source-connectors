// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.delete;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestContext;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.SimpleTrackedData;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class tests the DynamicDeleteTransactionOperation class.
 * It uses mock objects to simulate the behavior of the database connection and the operation request.
 */
public class DynamicDeleteTransactionOperationTest {
    public static final String EXECUTION_ID = "executionId";
    private static final String COLUMN_NAME_NAME = "customerId";
    private static final String INPUT = "{\"WHERE\":[{\"column\":\"customerId\",\"value\":\"111111\",\"operator\":\"=\"}]}";
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final TransactionDatabaseConnectorConnection _transactionDatabaseConnection = Mockito.mock(TransactionDatabaseConnectorConnection.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final DynamicDeleteTransactionOperation _dynamicDeleteTransactionOperation = new DynamicDeleteTransactionOperation(_transactionDatabaseConnection);
    private final DatabaseMetaData _databaseMetaDataForValidate = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(INPUT);
    private final SimpleOperationResponse _simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(_document));

    /**
     * This method sets up the test environment before each test.
     * It initializes the object under test and configures the behavior of the mock objects.
     */
    @Before
    public void setup() throws SQLException {
        Mockito.when(_transactionDatabaseConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_transactionDatabaseConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaDataForValidate);
        Mockito.when(_transactionDatabaseConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
    }

    /**
     * This test verifies that the getConnection method of the DynamicDeleteTransactionOperation class returns the correct object.
     */
    @Test
    public void getConnectionReturnsTransactionDatabaseConnectorConnection() {
        Assert.assertNotNull(_dynamicDeleteTransactionOperation.getConnection());
    }

    /**
     * This test verifies that the executeSizeLimitedUpdate method of the DynamicDeleteTransactionOperation class executes the update operation when joinTransaction is true.
     */
    @Test
    public void executeSizeLimitedUpdateExecutesUpdateOperationWhenJoinTransactionIsTrue() throws SQLException {
        setupTrackedData();
        Mockito.when(_updateRequest.getTopLevelExecutionId()).thenReturn("1234");
        Mockito.when(_transactionDatabaseConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaDataForValidate.getColumns(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TABLE_NAME)).thenReturn(
                DatabaseConnectorConstants.TABLE_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_transactionDatabaseConnection.createCacheKey(Mockito.anyString()))
                .thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        DynamicDeleteTransactionOperation dynamicDeleteTransactionOperation = new DynamicDeleteTransactionOperation(_transactionDatabaseConnection);
        dynamicDeleteTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        Mockito.verify(_connection, Mockito.never()).commit();;
    }

    /**
     * Test {@link DynamicDeleteTransactionOperation#executeSizeLimitedUpdate(UpdateRequest, OperationResponse)}
     */
    @Test
    public void testExecuteSizeLimitedUpdateValidateMetadata() throws SQLException {
        DynamicDeleteTransactionOperation dynamicDeleteTransactionOperation;
        OperationStatus operationStatus;
        setupTrackedData();
        Mockito.when(_updateRequest.getTopLevelExecutionId()).thenReturn("1234");
        Mockito.when(_transactionDatabaseConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaDataForValidate.getColumns(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(_resultSet);
        Mockito.when(_transactionDatabaseConnection.createCacheKey(Mockito.anyString()))
                .thenReturn(new TransactionCacheKey(EXECUTION_ID, _propertyMap));
        dynamicDeleteTransactionOperation = new DynamicDeleteTransactionOperation(_transactionDatabaseConnection);
        dynamicDeleteTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        List<SimplePayloadMetadata> payloadMetadata = _simpleOperationResponse.getResults().get(0)
                .getPayloadMetadatas();
        Map<String, String> trackedProps = payloadMetadata.get(0).getTrackedProps();
        Assert.assertTrue("Assert Transaction Id", trackedProps.get(TransactionConstants.TRANSACTION_ID).contains("TransactionId("));
        Assert.assertEquals("Assert Transaction Status", TransactionConstants.TRANSACTION_IN_PROGRESS,
                trackedProps.get(TransactionConstants.TRANSACTION_STATUS));
        Mockito.verify(_connection, Mockito.never()).commit();
    }

    /**
     * This method sets up the tracked data for the tests.
     */
    private void setupTrackedData() throws SQLException {

        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(13, result);
        Mockito.when(_databaseMetaDataForValidate.getColumns(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.any())).thenReturn(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, _simpleOperationResponse);
    }
}