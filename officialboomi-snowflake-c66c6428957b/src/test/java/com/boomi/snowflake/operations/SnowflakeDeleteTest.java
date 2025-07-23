// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.operations;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.OperationStatus;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.reflect.Whitebox;

import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.snowflake.util.ModifiedDeleteRequest;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.TestConfig;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.StringUtil;

@RunWith(Parameterized.class)
@PrepareForTest(SnowflakeDeleteOperation.class)
public class SnowflakeDeleteTest extends BaseTestOperation {
	private static final String TABLE_NAME = "DELETE_OPERATION_TESTER";
	private static final String EMPTY_JSON = "{}";
	private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"asd\"}";
	private static final String DELETE_JSON = "{\"ID\":\"3\"}";
	private static final String INCORRECT_FORMATTED_JSON = "{abc}";
	private static final long DEFAULT_BATCH_SIZE = 3;
	private static final int DOC_COUNT = 5;
	private final SortedMap<String, String> filterJSONObj=new TreeMap<>();
	private final SortedMap<String, String> metadata = new TreeMap<>();
	private List<String> _inputs;
	private ModifiedDeleteRequest _request;
	private ModifiedSimpleOperationResponse _response;
	private SnowflakeDeleteOperation _op;
	private SnowflakeDeleteOperation deleteOperation;
	private ConnectionProperties.ConnectionGetter connectionGetter;
	private Connection connection;
	private SnowflakeConnection snowflakeConnection;
	private Logger logger;
	private ResultSet resultSet;
	private DatabaseMetaData databaseMetaData;


	@Mock
	private ConnectionProperties connectionProperties;

	@Mock
	private DeleteRequest deleteRequest;

	@Mock
	private OperationResponse operationResponse;

	@Mock
	private SnowflakeWrapper snowflakeWrapper;

	@Mock
	private PreparedStatement preparedStatement;

	@Mock
	private ObjectIdData objectIdData;

	@Parameterized.Parameter
	public DynamicPropertyMap _dynamicPropertyMap;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB","DEV") },
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB","Not Present") },
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap(StringUtil.EMPTY_STRING,StringUtil.EMPTY_STRING) },
				{ new MutableDynamicPropertyMap() }
		});
	}

    @Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
		TestConfig.createDbAndSchema("DEF", "EMPLOYEES");
		testContext = new SnowflakeContextIT(OperationType.DELETE, null);
		testContext.setObjectTypeId(TABLE_NAME);
		testContext.setBatchSize(DEFAULT_BATCH_SIZE);
		testContext.addOperationProperty("columns", "ID");
		_response = new ModifiedSimpleOperationResponse();
		_op = Mockito.mock(SnowflakeDeleteOperation.class);
		connectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
		snowflakeConnection =Mockito.mock(SnowflakeConnection.class);
		deleteOperation = Mockito.spy(new SnowflakeDeleteOperation(snowflakeConnection));
		connection = Mockito.mock(Connection.class);
		logger = Logger.getAnonymousLogger();
		databaseMetaData = Mockito.mock(DatabaseMetaData.class);
		resultSet = Mockito.mock(ResultSet.class);
		Mockito.when(snowflakeConnection.getOperationContext())
				.thenReturn(new SnowflakeContextIT(OperationType.DELETE, null));
	}

	@Test
	public void shouldReturnSuccessWhenDeleting() {
		setInputs(DELETE_JSON);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 0, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}

	@Test
	public void shouldAddApplicationErrorWhenEmptyJsonEntered() {
		setInputs(EMPTY_JSON);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}

	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonFormatted() {
		setInputs(EMPTY_JSON);
		_inputs.set(3, INCORRECT_FORMATTED_JSON);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}

	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonValues() {
		setInputs(EMPTY_JSON);
		_inputs.set(3, INCORRECT_COL_NAME_JSON);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}

	private void setInputs(String value) {
		_inputs = new ArrayList<>();
		for(int i = 0 ; i < DOC_COUNT; i++) {
			_inputs.add(value);
		}
		_request = new ModifiedDeleteRequest(_inputs, _response);
	}

	/**
	 * Tests the `handleDelete` method to ensure it correctly handles delete operations
	 * based on dynamic properties and verifies database interactions and error handling.
	 * It checks the database and schema settings and validates the operation results or error responses.
	 */
	@Test
	public void TestHandleDelete() throws Exception {
		String jsonData = "{\"key1\":\"value1\",\"key2\":\"value2\"}";
		Mockito.when(deleteRequest.iterator()).thenReturn(Collections.singletonList(objectIdData).iterator());
		Mockito.when(objectIdData.getObjectId()).thenReturn(jsonData);
		Mockito.when(objectIdData.getLogger()).thenReturn(Logger.getLogger("test"));
		Mockito.when(snowflakeWrapper.constructDeleteStatement(ArgumentMatchers.any())).thenReturn(preparedStatement);
		Mockito.when(snowflakeWrapper.getPreparedStatement()).thenReturn(preparedStatement);
		Mockito.when(preparedStatement.getConnection()).thenReturn(TestConfig.getH2Connection());
		Mockito.when(objectIdData.getDynamicOperationProperties()).thenReturn(_dynamicPropertyMap);
		Whitebox.invokeMethod(_op, "handleDelete", deleteRequest, operationResponse,
				snowflakeWrapper, 1L, filterJSONObj, metadata);
		String db = _dynamicPropertyMap.getProperty("db");
		String schema = _dynamicPropertyMap.getProperty("schema");
		if (null == schema || schema.equals("DEV")) {
			if (null == schema) {
				Assert.assertNotEquals(db, preparedStatement.getConnection().getCatalog());
				Assert.assertNotEquals(schema, preparedStatement.getConnection().getSchema());
			} else {
				Assert.assertEquals(db, preparedStatement.getConnection().getCatalog());
				Assert.assertEquals(schema, preparedStatement.getConnection().getSchema());
			}
			Mockito.verify(snowflakeWrapper).constructDeleteStatement(filterJSONObj);
			Mockito.verify(snowflakeWrapper).fillStatementValuesWithDataType(preparedStatement, new TreeMap<>(),
					filterJSONObj, metadata);
			Mockito.verify(snowflakeWrapper).executeHandler(1L);
			Mockito.verify(operationResponse).addResult(ArgumentMatchers.eq(objectIdData),
					ArgumentMatchers.eq(OperationStatus.SUCCESS), ArgumentMatchers.eq("0"),
					ArgumentMatchers.isNull(), ArgumentMatchers.any());
		} else {
			ArgumentCaptor<String> errorMessageCaptor = ArgumentCaptor.forClass(String.class);
			ArgumentCaptor<Throwable> exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
			Mockito.verify(operationResponse).addErrorResult(ArgumentMatchers.eq(objectIdData),
					ArgumentMatchers.eq(OperationStatus.FAILURE), ArgumentMatchers.eq(""),
					errorMessageCaptor.capture(),
					exceptionCaptor.capture());

			String capturedErrorMessage = errorMessageCaptor.getValue();
			Throwable capturedException = exceptionCaptor.getValue();
			Assert.assertNotNull(capturedErrorMessage);
			Assert.assertTrue(capturedException instanceof JdbcSQLSyntaxErrorException);
			Assert.assertTrue(capturedErrorMessage.contains("Schema"));
			Assert.assertTrue(capturedErrorMessage.contains("not found"));
		}
		TestConfig.closeConnection();
	}

	/**
	 * Tests the behavior of the `executeDelete` method in the `SnowflakeDeleteOperation` class.
	 * This test verifies that the method interacts correctly with the `Connection` object and retrieves the expected columns
	 * from the database metadata. It ensures that the `getColumns` method is called with the correct parameters.
	 *
	 * @throws Exception if any exception occurs during the test execution
	 */
	@Test
	public void testExecuteDelete() throws Exception {
		Mockito.when(deleteRequest.iterator()).thenReturn(Collections.singletonList(objectIdData).iterator());
		Mockito.when(objectIdData.getLogger()).thenReturn(Logger.getLogger("test"));
		Mockito.when(operationResponse.getLogger()).thenReturn(Logger.getLogger("executeDelete()"));
		Mockito.when(connectionProperties.getConnectionGetter()).thenReturn(connectionGetter);
		Mockito.when(snowflakeConnection.getContext()).thenReturn(testContext);
		Mockito.when(connectionProperties.getBatchSize()).thenReturn(1L);
		Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		Mockito.when(connection.getMetaData()).thenReturn(databaseMetaData);
		Mockito.when(connection.getCatalog()).thenReturn("TEST_CATALOG");
		Mockito.when(connection.getSchema()).thenReturn("TEST_SCHEMA");
		Mockito.when(connection.getMetaData().getColumns(connection.getCatalog(), connection.getSchema(),
				TABLE_NAME,"%")).thenReturn(resultSet);
		deleteOperation.executeDelete(deleteRequest, operationResponse);
		Mockito.verify(databaseMetaData).getColumns("TEST_CATALOG", "TEST_SCHEMA", TABLE_NAME,
				"%");
	}

	/**
	 * Tests that `executeDelete` throws a {@link ConnectorException} when an error occurs.
	 */
	@Test(expected = ConnectorException.class)
	public void testExecuteDeleteException() {
		Mockito.when(deleteRequest.iterator()).thenReturn(Collections.singletonList(objectIdData).iterator());
		Mockito.when(objectIdData.getLogger()).thenReturn(Logger.getLogger("test"));
		Mockito.when(operationResponse.getLogger()).thenReturn(Logger.getLogger("executeDelete()"));
		Mockito.when(connectionProperties.getConnectionGetter()).thenReturn(connectionGetter);
		Mockito.when(snowflakeConnection.getContext()).thenReturn(testContext);
		Mockito.when(connectionProperties.getBatchSize()).thenReturn(1L);
		Mockito.when(snowflakeConnection.createJdbcConnection()).thenReturn(connection);
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		deleteOperation.executeDelete(deleteRequest, operationResponse);
	}
}