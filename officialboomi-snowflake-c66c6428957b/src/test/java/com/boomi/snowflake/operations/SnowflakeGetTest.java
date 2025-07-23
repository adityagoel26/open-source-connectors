// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.operations;

import com.boomi.connector.api.GetRequest;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.JSONHandler;
import com.boomi.snowflake.util.ModifiedGetRequest;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.SnowflakeContextIT;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

@RunWith(PowerMockRunner.class)
@PrepareForTest(JSONHandler.class)
public class SnowflakeGetTest extends BaseTestOperation {
	private static final String TABLE_NAME = "GET_OPERATION_TESTER";
	private static final String EMPTY_JSON = "{}";
	private static final String FILTER_JSON = "{\"ID\":\"%s\"}";
	private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"%s\"}";
	private static final long DEFAULT_BATCH_SIZE = 3;
	private static final int DOC_COUNT = 1;
	private ModifiedSimpleOperationResponse _response;
	private ModifiedGetRequest _request;
	private SnowflakeGetOperation _op;
	private SnowflakeConnection _snowflakeConnection;
	private GetRequest _getRequest;
	private OperationResponse _getResponse;
	private Logger logger;
	private ConnectionProperties connectionProperties;
	private ObjectData objectData;
	private Connection connection;
	private ConnectionProperties.ConnectionGetter connectionGetter;
	private DatabaseMetaData databaseMetaData;
	private ResultSet resultSet;

	@Before
	public void setup() {
		testContext = new SnowflakeContextIT(OperationType.GET, null);
		testContext.setObjectTypeId(TABLE_NAME);
		testContext.setBatchSize(DEFAULT_BATCH_SIZE);
		_response = new ModifiedSimpleOperationResponse();
		_getRequest = Mockito.mock(GetRequest.class);
		_getResponse = Mockito.mock(OperationResponse.class);
		_op = Mockito.mock(SnowflakeGetOperation.class);
		_snowflakeConnection = Mockito.mock(SnowflakeConnection.class);
		connection = Mockito.mock(Connection.class);
		connectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
		databaseMetaData = Mockito.mock(DatabaseMetaData.class);
		resultSet = Mockito.mock(ResultSet.class);
		connectionProperties = Mockito.mock(ConnectionProperties.class);
	}
	
	@Test
	public void shouldReturnAllTableData() {
		_request = new ModifiedGetRequest(EMPTY_JSON, _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 7, OperationStatus.SUCCESS);
		assertOperation(OperationStatus.SUCCESS, 7, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldReturnTwoRows() {
		_request = new ModifiedGetRequest(String.format(FILTER_JSON, 3), _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 2, OperationStatus.SUCCESS);
		assertOperation(OperationStatus.SUCCESS, 2, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldReturnEmptySuccess() {
		_request = new ModifiedGetRequest(String.format(FILTER_JSON, -1), _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.SUCCESS);
		assertOperation(OperationStatus.SUCCESS, 0, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenIncorrectColNameGiven() {
		_request = new ModifiedGetRequest(String.format(INCORRECT_COL_NAME_JSON, -1), _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddFailureWhenIncorrectInputFormatGiven() {
		_request = new ModifiedGetRequest(TABLE_NAME, _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.FAILURE);
		assertOperation(OperationStatus.FAILURE, 0, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenNonPositiveBatchSize() {
		testContext.setBatchSize(0);
		_request = new ModifiedGetRequest(EMPTY_JSON, _response);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, 1);
		assertConnectionIsClosed(_op.getConnection());
	}

	/**
	 * Tests the behavior of the `executeGet` method in the `SnowflakeGetOperation` class.
	 * This test verifies that the method correctly interacts with the mocked `JSONHandler`, `SnowflakeConnection`,
	 * `Logger`, and other dependencies, and ensures that the expected methods are called with the correct arguments.
	 */
	@Test
	public void testExecuteGet() throws Exception {
		SimpleTrackedData inputDoc1 = new SimpleTrackedData(1,objectData );
		SortedMap<String, String> mockSortedMap = new TreeMap<>();
		mockSortedMap.put("exampleKey", "exampleValue");
		PowerMockito.mockStatic(JSONHandler.class);
		logger = Logger.getAnonymousLogger();
		objectData = Mockito.mock(ObjectData.class);
		_op = PowerMockito.spy(new SnowflakeGetOperation(_snowflakeConnection));
		Mockito.when(_snowflakeConnection.getOperationContext())
				.thenReturn(new SnowflakeContextIT(OperationType.GET, null));
		Mockito.when(_getResponse.getLogger()).thenReturn(logger);
		PowerMockito.when(JSONHandler.readSortedMap("testObjectId")).thenReturn(mockSortedMap);
		Mockito.when(_getRequest.getObjectId()).thenReturn(inputDoc1);
		Mockito.when(connectionProperties.getConnectionGetter()).thenReturn(connectionGetter);
		Mockito.when(_snowflakeConnection.getContext()).thenReturn(testContext);
		Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
		Mockito.when(connection.getCatalog()).thenReturn("TEST_CATALOG");
		Mockito.when(connection.getSchema()).thenReturn("TEST_SCHEMA");
		Mockito.when(connection.getMetaData().getColumns(connection.getCatalog(), connection.getSchema(),
				TABLE_NAME,"%")).thenReturn(resultSet);

		_op.executeGet(_getRequest, _getResponse);
		Mockito.verify(databaseMetaData).getColumns("TEST_CATALOG", "TEST_SCHEMA", TABLE_NAME,
				"%");
	}
}
