// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.update;

// Importing necessary libraries and classes

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.databaseconnector.cache.TransactionCacheKey;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.TransactionConstants;
import com.boomi.connector.databaseconnector.operations.StandardOperation;
import com.boomi.connector.databaseconnector.operations.StandardTransactionOperation;
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
import java.sql.SQLException;
import java.util.Collections;

/**
 * This class is a test class for the StandardUpdateTransactionOperation.
 * It uses JUnit for testing and Mockito for mocking dependencies.
 */
public class StandardUpdateTransactionOperationTest {

    // Declaring constants
    private static final String OBJECT_TYPE_ID = "TEST";
    private final static String SCHEMA_NAME_REF = "Schema Name";
    private final static String DATABASE_NAME = "Microsoft SQL Server";
    private final static String CATALOG = "Catalog";
    private static final long BATCH_COUNT_LONG = 10L;
    private static final String INPUT = "{\"WHERE\":[{\"column\":\"customerId\",\"value\":\"111111\",\"operator\":\"=\"}]}";
    private final static String STATUS_ERROR_MESSAGE = "Response status is not SUCCESS";
    private final static String TRANSACTION_STATUS_ERROR_MESSAGE = "Transaction status is not valid";
    private final static String TRANSACTION_ID_ERROR_MESSAGE = "Transaction id is not valid";

    // Mocking necessary classes for testing
    private final TransactionDatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(TransactionDatabaseConnectorConnection.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaDataForSQL = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final TransactionCacheKey transactionCacheKey = new TransactionCacheKey(ResponseUtil.EXECUTION_ID, null);

    // Declaring other necessary variables
    private final int[] intArray = new int[1];
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final StandardTransactionOperation _standardTransactionOperation = new StandardTransactionOperation(_databaseConnectorConnection);
    private final StandardOperation _standardOperation = new StandardOperation(_databaseConnectorConnection);
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(INPUT);
    private final SimpleOperationResponse _simpleOperationResponse =  ResponseUtil.getResponse(Collections.singletonList(_document));

    /**
     * This method sets up the necessary conditions for the tests.
     * It is annotated with @Before, so it runs before each test method.
     */
    @Before
    public void setup() {
        // Setting up the necessary conditions for the tests
        Mockito.when(_standardOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_standardTransactionOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_databaseConnectorConnection.createCacheKey(ResponseUtil.EXECUTION_ID)).thenReturn(transactionCacheKey);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection(transactionCacheKey)).thenReturn(_connection);
        Mockito.when(_databaseConnectorConnection.getReadTimeOut()).thenReturn(BATCH_COUNT_LONG);
    }

    /**
     * This test checks if the getConnection method returns the correct object.
     */
    @Test
    public void getConnectionReturnsTransactionDatabaseConnectorConnection() {
        StandardTransactionOperation standardTransactionOperation = new StandardTransactionOperation(_databaseConnectorConnection);
        Assert.assertNotNull(standardTransactionOperation.getConnection());
    }

    /**
     * This test checks if the executeSizeLimitedUpdate method executes the update operation when joinTransaction is true.
     */
    @Test
    public void executeSizeLimitedUpdateExecutesUpdateOperationWhenJoinTransactionIsTrue() throws SQLException {
        setupDataForExecuteOperation();

        StandardTransactionOperation standardTransactionOperation = new StandardTransactionOperation(_databaseConnectorConnection);
        standardTransactionOperation.executeSizeLimitedUpdate(ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);

        SimpleOperationResult simpleOperationResultList = _simpleOperationResponse.getResults().get(0);

        Mockito.verify(_databaseConnectorConnection, Mockito.times(1)).getDatabaseConnection(Mockito.any());
        Assert.assertEquals(STATUS_ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResultList.getStatus());
        Assert.assertEquals(TRANSACTION_STATUS_ERROR_MESSAGE, TransactionConstants.TRANSACTION_IN_PROGRESS, simpleOperationResultList.getPayloadMetadatas().get(0).getTrackedProps().get(TransactionConstants.TRANSACTION_STATUS));
        Assert.assertEquals(TRANSACTION_ID_ERROR_MESSAGE, transactionCacheKey.toString(), simpleOperationResultList.getPayloadMetadatas().get(0).getTrackedProps().get(TransactionConstants.TRANSACTION_ID));
    }

    /**
     * This method sets up the necessary data for the executeOperation tests.
     */
    public void setupDataForExecuteOperation() throws SQLException {
        // Setting up the necessary data for the executeOperation tests
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaDataForSQL);
        Mockito.when(_databaseMetaDataForSQL.getDatabaseProductName()).thenReturn(DATABASE_NAME);
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_databaseMetaDataForSQL.getColumns(CATALOG, SCHEMA_NAME_REF, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);

        DataTypesUtil.setUpResultObjectData(_resultSet);

        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(intArray);

    }
}