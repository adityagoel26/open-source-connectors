// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.operations;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.override.ConnectionOverrideUtil;
import com.boomi.snowflake.util.ConnectionProperties;

import net.snowflake.client.jdbc.SnowflakeStatement;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeContextIT;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ConnectionOverrideUtil.class})
public class SnowflakeExecuteTest extends BaseTestOperation {
	private static final String OPERATION_NAME = "EXECUTE";
	private static final String SP_NAME = "test_DB.PUBLIC.EXECUTE_OPERATION_TESTER.(P1 VARCHAR, P2 VARCHAR).VA";
	private static final String EMPTY_JSON = "{}";
	private static final String ONE_PARAMETER = "{\"P1\":\"v1\"}";
	private static final int DOC_COUNT = 5;
	private static final long DEFAULT_BATCH_SIZE = 1;
	private static final String INCORRECT_FORMATTED_JSON = "{abc}";
	private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"asd\"}";
	private SnowflakeExecuteOperation _op;
	private ModifiedUpdateRequest _request;
	private ModifiedSimpleOperationResponse _response;
	private SnowflakeConnection _connection;
	private OperationResponse mockResponse;
	private ConnectionProperties connectionProperties;
	private ConnectionProperties.ConnectionGetter connectionGetter;
	private Logger logger;
	private Connection connection;
	private DynamicPropertyMap dynamicPropertyMap;
	private Statement statement;
	private SnowflakeStatement snowflakeStatement;
    private List<InputStream> _inputs;


    @Before
	public void setup() {
		testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
		testContext.setObjectTypeId(SP_NAME);
		testContext.setBatchSize(DEFAULT_BATCH_SIZE);
		_response = new ModifiedSimpleOperationResponse();
		_connection = PowerMockito.mock(SnowflakeConnection.class);
		mockResponse = Mockito.mock(OperationResponse.class);
		connectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
		connection = Mockito.mock(Connection.class);
		dynamicPropertyMap = Mockito.mock(DynamicPropertyMap.class);
		logger = Mockito.mock(Logger.class);
		statement = Mockito.mock(Statement.class);
		snowflakeStatement = Mockito.mock(SnowflakeStatement.class);
		_op = Mockito.spy(new SnowflakeExecuteOperation(_connection));
		Mockito.when(_connection.getOperationContext())
				.thenReturn(new SnowflakeContextIT(OperationType.EXECUTE, "EXECUTE"));
		connectionProperties = new ConnectionProperties(_connection, new MutablePropertyMap(), "EMPLOYEE",
				Logger.getAnonymousLogger());
		Mockito.when(_connection.getContext()).thenReturn(testContext);
	}
	
	@Test
	public void shouldReturnSuccessWhenSendingOneParameter() {
		setInputs(new ByteArrayInputStream(ONE_PARAMETER.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldReturnSuccessWhenInsertingAllNulls() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonFormatted() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_FORMATTED_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonValues() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}
	
	
	private void setInputs(InputStream value) {
		_inputs = new ArrayList<>();
		for(int i = 0 ; i < DOC_COUNT; i++) {
			_inputs.add(value);
		}
		_request = new ModifiedUpdateRequest(_inputs, _response);
	}

	/**
	 * Validates that `executeSizeLimitedUpdate` throws a {@link ConnectorException}
	 * when the request data contains invalid JSON.
	 *
	 * @throws Exception if an unexpected error occurs during test execution.
	 */
	@Test
	public void testExecuteSizeLimitedUpdateWhenRequestDataIsNotEmpty() throws Exception {
		String validJsonData = "{\"P1\": \"value1\", \"P2\": \"value2\"}";
		String validJsonData1 = "{\"P1\": \"value1\", \"P2\": \"value2\"}";
		String inValidJsonData = "{Invalid Json}";
		List<ObjectData> listofObjectData = new ArrayList<>();
		SimpleTrackedData requestData1 = new SimpleTrackedData(1,  new ByteArrayInputStream(validJsonData.getBytes()));
		SimpleTrackedData requestData2 = new SimpleTrackedData(2,  new ByteArrayInputStream(validJsonData1.getBytes()));
		SimpleTrackedData requestData3 = new SimpleTrackedData(2,  new ByteArrayInputStream(inValidJsonData.getBytes()));
		listofObjectData.add(requestData1);
		listofObjectData.add(requestData2);
		listofObjectData.add(requestData3);
		SimpleUpdateRequest simpleUpdateRequest =  new SimpleUpdateRequest(listofObjectData);
		Mockito.when(mockResponse.getLogger()).thenReturn(Logger.getAnonymousLogger());
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		Mockito.when(_connection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(null,dynamicPropertyMap )).thenReturn(connection);
		Mockito.when(connection.createStatement()).thenReturn(statement);
		Mockito.when(statement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);
		_op.executeSizeLimitedUpdate(simpleUpdateRequest, mockResponse);
		Mockito.verify(statement, Mockito.times(3)).execute(Mockito.anyString());

	}
	/**
	 * Validates that `executeSizeLimitedUpdate` correctly processes non-empty request data.
	 * Verifies that the `execute` method of the statement is called twice when the request data is valid.
	 *
	 * @throws Exception if an unexpected error occurs during test execution.
	 */
	@Test
	public void testExecuteSizeLimitedUpdateWhenRequestDataIsEmpty() throws Exception {
		String validJsonData = "{\"P1\": \"value1\", \"P2\": \"value2\"}";
		String validJsonData1 = "{\"P1\": \"value1\", \"P2\": \"value2\"}";
		List<ObjectData> listofObjectData = new ArrayList<>();
		SimpleTrackedData requestData1 = new SimpleTrackedData(1,  new ByteArrayInputStream(validJsonData.getBytes()));
		SimpleTrackedData requestData2 = new SimpleTrackedData(2,  new ByteArrayInputStream(validJsonData1.getBytes()));
		listofObjectData.add(requestData1);
		listofObjectData.add(requestData2);
		SimpleUpdateRequest simpleUpdateRequest =  new SimpleUpdateRequest(listofObjectData);
		Mockito.when(mockResponse.getLogger()).thenReturn(Logger.getAnonymousLogger());
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		Mockito.when(_connection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(null,dynamicPropertyMap )).thenReturn(connection);
		Mockito.when(connection.createStatement()).thenReturn(statement);
		Mockito.when(statement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);
		_op.executeSizeLimitedUpdate(simpleUpdateRequest, mockResponse);
		Mockito.verify(statement, Mockito.times(2)).execute(Mockito.anyString());
	}

	/**
	 * Validates that `executeSizeLimitedUpdate` correctly handles the case when
	 * override is enabled, and the request data array is not empty. Verifies that
	 * `ConnectionOverrideUtil.isOverrideEnabled` is called once.
	 *
	 * @throws Exception if an unexpected error occurs during test execution.
	 */
	@Test
	public void testExecuteSizeLimitedUpdateWheIsOverrideEnable() throws Exception {
		String validJsonData = "{\"P1\": \"value1\", \"P2\": \"value2\"}";
		String inValidJsonData = "{Invalid Json}";
		List<ObjectData> listofObjectData = new ArrayList<>();
		SimpleTrackedData requestData1 = new SimpleTrackedData(1, new ByteArrayInputStream(validJsonData.getBytes()));
		SimpleTrackedData requestData2 = new SimpleTrackedData(2, new ByteArrayInputStream(inValidJsonData.getBytes()));
		listofObjectData.add(requestData1);
		listofObjectData.add(requestData2);
		SimpleUpdateRequest simpleUpdateRequest = new SimpleUpdateRequest(listofObjectData);
		Mockito.when(mockResponse.getLogger()).thenReturn(Logger.getAnonymousLogger());
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		Mockito.when(_connection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(null, dynamicPropertyMap)).thenReturn(connection);
		Mockito.when(connection.createStatement()).thenReturn(statement);
		Mockito.when(statement.unwrap(SnowflakeStatement.class)).thenReturn(snowflakeStatement);

		_op.executeSizeLimitedUpdate(simpleUpdateRequest, mockResponse);
		Mockito.verify(statement, Mockito.times(2)).execute(Mockito.anyString());
	}
}
