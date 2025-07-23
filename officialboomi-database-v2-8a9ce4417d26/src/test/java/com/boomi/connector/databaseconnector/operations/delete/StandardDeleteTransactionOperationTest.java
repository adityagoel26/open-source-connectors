// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.delete;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.TransactionDatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.databaseconnector.operations.StandardTransactionOperation;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestContext;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
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
import java.util.Iterator;

/**
 * This class is a test class for the StandardUpdateTransactionOperation.
 * It uses JUnit for testing and Mockito for mocking dependencies.
 */
public class StandardDeleteTransactionOperationTest {

    // Declaring constants
    private static final String OBJECT_TYPE_ID = "EVENT";
    private final static String SCHEMA_NAME_REF = "Schema Name";
    private final static String DATABASE_NAME = "Microsoft SQL Server";
    private final static String CATALOG = "Catalog";
    private final int[] intArray = new int[1];
    private static final String INPUT = "{\"WHERE\":[{\"column\":\"customerId\",\"value\":\"111111\",\"operator\":\"=\"}]}";
    private final OperationContext _operationContext = DatabaseConnectorTestContext.getDatabaseContext();
    private final TransactionDatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(TransactionDatabaseConnectorConnection.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaDataForSQL = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Iterator<ObjectData> _objectDataIterator = Mockito.mock(Iterator.class);
    private final StandardTransactionOperation standardTransactionOperation = new StandardTransactionOperation(_databaseConnectorConnection);
    private final SimpleTrackedData _document = ResponseUtil.createInputDocument(INPUT);
    private final SimpleOperationResponse _simpleOperationResponse = ResponseUtil.getResponse(Collections.singleton(_document));

    /**
     * This method sets up the necessary conditions for the tests.
     * It is annotated with @Before, so it runs before each test method.
     */
    @Before
    public void setup() {
        // Setting up the necessary conditions for the tests
        Mockito.when(_databaseConnectorConnection.getContext()).thenReturn(_operationContext);
    }

    /**
     * This test checks if the getConnection method returns the correct object.
     */
    @Test
    public void getConnectionReturnsTransactionDatabaseConnectorConnection() {
        Assert.assertNotNull(standardTransactionOperation.getConnection());
    }

    /**
     * This test checks if the executeSizeLimitedUpdate method executes the update operation when joinTransaction is true.
     */
    @Test
    public void executeSizeLimitedUpdateExecutesUpdateOperationWhenJoinTransactionIsTrue() throws SQLException {
        setupDataForExecuteOperation();
        Mockito.when(_updateRequest.getTopLevelExecutionId()).thenReturn("1234");
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection(Mockito.any())).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaDataForSQL);
        Mockito.when(_databaseMetaDataForSQL.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_databaseMetaDataForSQL.getColumns(Mockito.nullable(String.class), Mockito.nullable(String.class),
                Mockito.nullable(String.class), Mockito.nullable(String.class))).thenReturn(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TABLE_NAME)).thenReturn(
                DatabaseConnectorConstants.TABLE_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        StandardTransactionOperation standardTransactionOperation = new StandardTransactionOperation(_databaseConnectorConnection);
        standardTransactionOperation.executeSizeLimitedUpdate(
                ResponseUtil.toRequest(Collections.singleton(_document)), _simpleOperationResponse);
        Mockito.verify(_connection, Mockito.never()).commit();
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

        InputStream result = new ByteArrayInputStream(DataTypesUtil.INPUT.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(11, result);

        Mockito.when(_objectDataIterator.next()).thenReturn(trackedData);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, _simpleOperationResponse);
    }
}