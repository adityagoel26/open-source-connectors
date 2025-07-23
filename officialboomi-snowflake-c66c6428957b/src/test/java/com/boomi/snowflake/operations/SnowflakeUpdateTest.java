// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.operations;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ConnectionProperties;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.reflect.Whitebox;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.override.ConnectionOverrideUtilTest;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.TestConfig;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
import com.boomi.util.StringUtil;

@RunWith(Parameterized.class)
public class SnowflakeUpdateTest extends BaseTestOperation {
	private static final String TABLE_NAME = "UPDATE_OPERATION_TESTER";
	private static final String EMPTY_JSON = "{}";
	private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"asd\"}";
	private static final String UPDATE_JSON = "{\"ID\":\"3\",\"Name\":\"new name\"}";
	private static final String INCORRECT_FORMATTED_JSON = "{abc}";
	private static final long DEFAULT_BATCH_SIZE = 3;
	private static final int DOC_COUNT = 5;
	private List<InputStream> _inputs;
	private ModifiedUpdateRequest _request;
	private ModifiedSimpleOperationResponse _response;
	private SnowflakeUpdateOperation _op;
	private UpdateRequest _updateRequest;
	private OperationResponse _updateResponse;
	private SnowflakeUpdateOperation _snowflakeUpdateOperation;


	@Mock
	private ConnectionProperties connectionProperties;
	@Mock
	private SnowflakeConnection _snowflakeConnection;
	@Mock
	private SnowflakeWrapper mockWrapper;
	@Mock
	private ConnectionProperties.ConnectionGetter connectionGetter;
	@Mock
	private Logger mockLogger;
	@Mock
	private Connection mockConnection;
	@Mock
	private ResultSet mockResultSet;
	@Mock
	private DatabaseMetaData mockDatabaseMetaData;
	@Mock
	private ObjectData mockObjectData;


	@Parameterized.Parameter
	public MutableDynamicPropertyMap _dynamicPropertyMap;

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB","DEV") },
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB","Not Present") },
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap("TEST_SF_DB",StringUtil.EMPTY_STRING) },
				{ ConnectionOverrideUtilTest.createDynamicPropertyMap(StringUtil.EMPTY_STRING,StringUtil.EMPTY_STRING) },
				{ new MutableDynamicPropertyMap() }
		});
	}

	@Before
	public void setup() {
		MockitoAnnotations.initMocks(this);
		TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
		TestConfig.createDbAndSchema("DEF", "EMPLOYEES");
		testContext = new SnowflakeContextIT(OperationType.UPDATE, null);
		testContext.setObjectTypeId(TABLE_NAME);
		testContext.setBatchSize(DEFAULT_BATCH_SIZE);
		testContext.addOperationProperty("columns", "ID");
		_response = new ModifiedSimpleOperationResponse();
		_updateRequest = Mockito.mock(UpdateRequest.class);
		_updateResponse = Mockito.mock(OperationResponse.class);
		_op = Mockito.mock(SnowflakeUpdateOperation.class);
	}
	
	@Test
	public void shouldReturnSuccessWhenUpdatingOneCol() {
		setInputs(new ByteArrayInputStream(UPDATE_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	@Test
	public void shouldAddApplicationErrorWhenEmptyJsonEntered() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
		runSnowflakeOperation(_op, _request, response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		results = response.getResults();
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonFormatted() {
		testContext.addOperationProperty("invalidField", "Invalid Value");
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_FORMATTED_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}
	
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonValues() {
		testContext.addConnectionProperty("invalidField", "Invalid Value");
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
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
	 * Tests the execution of the `executeUpdate` method.
	 * Verifies interaction with `ConnectionOverrideUtil`.
	 */
	@Test
	public void testExecuteUpdate() throws Exception {
		SortedMap<String, String> filterObj = new TreeMap<>();
		SortedMap<String, String> cpy = new TreeMap<>();
		SortedMap<String, String> metadata = new TreeMap<>();
		Long batchSize = 10L;

		setInputs(new ByteArrayInputStream(UPDATE_JSON.getBytes()));
		SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0), null, null,
				_dynamicPropertyMap);
		SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
		SimpleOperationResponse response = new SimpleOperationResponse();
		response.addTrackedData(inputDoc);

		Connection connection = TestConfig.getH2Connection();
		PreparedStatement preparedStatement = connection.prepareStatement(
				"UPDATE EMPLOYEES SET \"NAME\" =  ?  WHERE \"ID\" LIKE  ?");
		Mockito.when(mockWrapper.getPreparedStatement()).thenReturn(preparedStatement);
		Mockito.doNothing().when(mockWrapper).executeHandler(ArgumentMatchers.anyLong());

		Whitebox.invokeMethod(_op, "executeUpdate", request, response, mockWrapper, batchSize,
				filterObj, cpy, metadata);

		if (response.getResults().get(0).getStatus().equals(OperationStatus.SUCCESS)) {
			assertSuccessfulOperation(_dynamicPropertyMap, connection);
		} else {
			assertFailedOperation(response.getResults().get(0));
		}
		TestConfig.closeConnection();
	}

	private void assertSuccessfulOperation(MutableDynamicPropertyMap dynamicPropertyMap, Connection connection)
			throws SQLException {
		String schema = dynamicPropertyMap.getProperty("schema");
		String db = dynamicPropertyMap.getProperty("db");
		if (schema == null || db == null) {
			Assert.assertNotEquals("Database catalog should not update when db is null", db,
					connection.getSchema());
			Assert.assertNotEquals("Schema should not update when schema is null", schema,
					connection.getSchema());
		} else {
			Assert.assertEquals("Database catalog should match", db, connection.getCatalog());
			Assert.assertEquals("Schema should match", schema, connection.getSchema());
		}
	}

	private void assertFailedOperation(SimpleOperationResult result) {
		Assert.assertEquals("Operation status should be FAILURE", OperationStatus.FAILURE, result.getStatus());
		Assert.assertTrue("Error message should contain 'Schema'", result.getMessage().contains("Schema"));
		Assert.assertTrue("Error message should contain 'not found'", result.getMessage().contains("not found"));
	}
	/**
	 * Tests the behavior of the `executeSizeLimitedUpdate` method in the `SnowflakeUpdateOperation` class.
	 * This test verifies that the method correctly interacts with the mocked `SnowflakeConnection`, `Logger`,
	 * `ConnectionProperties`, `DatabaseMetaData`, and other dependencies, and ensures that the `getColumns` method
	 * on `DatabaseMetaData` is invoked with the expected arguments.
	 *
	 * @throws Exception if any exception occurs during the test execution
	 */
	@Test
	public void testExecuteSizeLimitedUpdate() throws Exception {
		_snowflakeUpdateOperation = Mockito.spy(new SnowflakeUpdateOperation(_snowflakeConnection));
		Mockito.when(_snowflakeConnection.getOperationContext())
				.thenReturn(new SnowflakeContextIT(OperationType.UPDATE, null));
		Mockito.when(_updateRequest.iterator()).thenReturn(Collections.singletonList(mockObjectData).iterator());
		Mockito.when(mockObjectData.getLogger()).thenReturn(Logger.getLogger("test"));
		Mockito.when(_updateResponse.getLogger()).thenReturn(Logger.getLogger("executeUpdate()"));
		Mockito.when(_snowflakeConnection.getContext()).thenReturn(testContext);
		Mockito.when(connectionProperties.getBatchSize()).thenReturn(1L);
		Mockito.when(_snowflakeConnection.createJdbcConnection()).thenReturn(mockConnection);
		Mockito.when(connectionGetter.getConnection(mockLogger)).thenReturn(mockConnection);
		Mockito.when(mockConnection.getMetaData()).thenReturn(mockDatabaseMetaData);
		Mockito.when(mockConnection.getCatalog()).thenReturn("TEST_CATALOG");
		Mockito.when(mockConnection.getSchema()).thenReturn("TEST_SCHEMA");
		Mockito.when(mockConnection.getMetaData().getColumns(mockConnection.getCatalog(), mockConnection.getSchema(),
				TABLE_NAME,"%")).thenReturn(mockResultSet);
		_snowflakeUpdateOperation.executeSizeLimitedUpdate(_updateRequest, _updateResponse);
		Mockito.verify(mockDatabaseMetaData).getColumns("TEST_CATALOG", "TEST_SCHEMA", TABLE_NAME,
				"%");
	}

}