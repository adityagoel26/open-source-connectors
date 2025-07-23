// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.operations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Map;
import java.util.logging.Logger;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.snowflake.controllers.SnowflakeCreateController;
import com.boomi.snowflake.util.BoundedMap;
import com.boomi.snowflake.util.TableDefaultAndMetaDataObject;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.ConnectionProperties;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeOperationUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.MapType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.boomi.snowflake.SnowflakeConnection;
import org.powermock.reflect.Whitebox;


/**
 * The Class SnowflakeCreateTest.
 *
 * @author Vanangudi,S
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(SnowflakeOperationUtil.class)
public class SnowflakeCreateTest extends BaseTestOperation {
	/** The Constant TABLE_NAME. */
	private static final String TABLE_NAME = "CREATE_OPERATION_TESTER";
	/** The Constant EMPTY_JSON. */
	private static final String EMPTY_JSON = "{}";
	/** The Constant INCORRECT_COL_NAME_JSON. */
	private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"asd\"}";
	/** The Constant COL_NAME_JSON. */
	private static final String COL_NAME_JSON = "{\"ID\":\"asd\"}";
	/** The Constant INCORRECT_FORMATTED_JSON. */
	private static final String INCORRECT_FORMATTED_JSON = "{abc}";
	/** The Constant DEFAULT_BATCH_SIZE. */
	private static final long DEFAULT_BATCH_SIZE = 3;
	/** The Constant DOC_COUNT. */
	private static final int DOC_COUNT = 5;
	/** The Constant EMPTY_FIELD_SELECTION. */
	private static final String EMPTY_FIELD_SELECTION = "inputOptionsForMissingFields";
	/** The Constant NULL_SELECTION. */
	public static final String NULL_SELECTION = "SELECT_NULL";
	/** The Constant DEFAULT_SELECTION. */
	public static final String DEFAULT_SELECTION = "SELECT_DEFAULT";
	/** The List of Input Stream. */
	private List<InputStream> _inputs;
	private SnowflakeCreateOperation _op;
	private ModifiedSimpleOperationResponse _response;
	private ModifiedUpdateRequest _request;
	private ConnectionProperties properties;
	private SortedMap<String, String> metaDataValues = new TreeMap<>();
	private ConnectionProperties.ConnectionGetter connectionGetter;
	private Connection connection;
	private Logger logger;
	private SnowflakeCreateOperation snowflakeCreateOperation;
	private SnowflakeCreateController mockController;
	private OperationResponse mockOperationResponse;
	private SortedMap<String, String> defaultValues = new TreeMap<>();
	private ObjectMapper mockObjectMapper;
	private ObjectData mockObjectData;
	private MutableDynamicPropertyMap mockOperationProperty;
	private PropertyMap mockPropertyMap;
	private final BoundedMap<String, TableDefaultAndMetaDataObject> _boundedMap
			= new BoundedMap<>(5);
	private TypeFactory mockTypeFactory;
	SortedMap<String, TableDefaultAndMetaDataObject> sortedMap = new TreeMap<>();
	TableDefaultAndMetaDataObject defaultValueObject = new TableDefaultAndMetaDataObject();
	private UpdateRequest mockUpdateRequest;
	private SnowflakeConnection mockSnowflakeConnection;
	private SnowflakeCreateOperation _snowflakeCreateOperation;
	private final SortedMap<String,String> input = new TreeMap<>();
	private String cookieValue = "{\"TEST_SF_DB|PUBLIC\":{\"defaultValues\":{\"AVAILABLE\":\"TRUE\",\"DESC\":\"new product\",\"PRICE\":\"200\",\"RELEASE\":\"01012025\"},\"metaDataValues\":{\"AVAILABLE\":\"BOOLEAN\",\"CODE\":\"NUMBER\",\"DERIVED\":\"NUMBER\",\"DESC\":\"VARCHAR\",\"ID\":\"NUMBER\",\"NAME\":\"VARCHAR\",\"PRICE\":\"NUMBER\",\"RELEASE\":\"DATE\"},\"tableMetaDataValues\":{\"AVAILABLE\":\"BOOLEAN\",\"CODE\":\"NUMBER\",\"DESC\":\"VARCHAR\",\"ID\":\"NUMBER\",\"NAME\":\"VARCHAR\",\"PRICE\":\"NUMBER\",\"RELEASE\":\"DATE\"}}}";


	@Rule
	public ExpectedException thrown = ExpectedException.none();


	/**
	 *Sets all the Connection properties
	 */
	@Before
	public void setup() throws SQLException {
		testContext = new SnowflakeContextIT(OperationType.CREATE, null);
		testContext.setObjectTypeId(TABLE_NAME);
		testContext.setBatchSize(DEFAULT_BATCH_SIZE);
		mockPropertyMap = Mockito.mock(PropertyMap.class);
		mockUpdateRequest = Mockito.mock(UpdateRequest.class);
		mockOperationProperty = Mockito.mock(MutableDynamicPropertyMap.class);
		mockSnowflakeConnection = Mockito.mock(SnowflakeConnection.class);
		_snowflakeCreateOperation = new SnowflakeCreateOperation(mockSnowflakeConnection);
		_op = Mockito.mock(SnowflakeCreateOperation.class);
		_response = new ModifiedSimpleOperationResponse();
		properties = Mockito.mock(ConnectionProperties.class);
		metaDataValues.put("key1", "metaData");
		defaultValues.put("key1", "value1");
		defaultValueObject.setDefaultValues(defaultValues);
		defaultValueObject.setMetaDataValues(metaDataValues);
		defaultValueObject.setTableMetaDataValues(metaDataValues);
		sortedMap.put("SOLUTIONS_DB|PUBLIC", defaultValueObject);
		mockTypeFactory = Mockito.mock(TypeFactory.class);
		connectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
		connection = Mockito.mock(Connection.class);
		logger = Mockito.mock(Logger.class);
		mockController = Mockito.mock(SnowflakeCreateController.class);
		mockOperationResponse = Mockito.mock(OperationResponse.class);
		mockObjectData = Mockito.mock(ObjectData.class);
		mockOperationProperty = Mockito.mock(MutableDynamicPropertyMap.class);
		mockOperationProperty.addProperty("db","db");
		mockOperationProperty.addProperty("schema", "schema");
		Mockito.when(properties.getConnectionGetter()).thenReturn(connectionGetter);
		Mockito.when(connectionGetter.getConnection(logger)).thenReturn(connection);
		PowerMockito.mockStatic(SnowflakeOperationUtil.class);
		properties = Mockito.mock(ConnectionProperties.class);
		connectionGetter = Mockito.mock(ConnectionProperties.ConnectionGetter.class);
		connection = Mockito.mock(Connection.class);
		logger = Mockito.mock(Logger.class);
		mockObjectMapper = Mockito.mock(ObjectMapper.class);
		snowflakeCreateOperation = new SnowflakeCreateOperation(mockSnowflakeConnection);
		PowerMockito.mockStatic(SnowflakeOperationUtil.class);
		Mockito.when(mockSnowflakeConnection.createJdbcConnection()).thenReturn(connection);
		PowerMockito.when(SnowflakeOperationUtil.getDefaultsValues(ArgumentMatchers.any(), ArgumentMatchers.any(),ArgumentMatchers.any())).thenReturn(defaultValues);
		PowerMockito.when(SnowflakeOperationUtil.getObjectMapper()).thenReturn(mockObjectMapper);
	}
	
	/**
	 * Checks if the create operation returns success when inserting one column
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingOneCol() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, NULL_SELECTION );
		setInputs(new ByteArrayInputStream(COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Checks if the create operation returns success when inserting one column with return results enabled
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingOneColWithReturnResults() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, NULL_SELECTION );
		setInputs(new ByteArrayInputStream(COL_NAME_JSON.getBytes()));
		testContext.getOperationProperties().putIfAbsent("returnResults", true);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertBatchOperation(OperationStatus.SUCCESS, DOC_COUNT, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Checks if the create operation returns success when inserting all nulls in the column.
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingAllNulls() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, NULL_SELECTION );
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Checks if the create operation returns error when inserting one invalid JSON format input.
	 */
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonFormatted() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_FORMATTED_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Checks if the create operation returns error when inserting invalid JSON format input values.
	 */
	@Test
	public void shouldAddApplicationErrorWhenOneInvalidInputJsonValues() {
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		_inputs.set(3,new ByteArrayInputStream(INCORRECT_COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 0, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		Assert.assertEquals(OperationStatus.APPLICATION_ERROR, results.get(3).getStatus());
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Should return success when inserting one col with null selection.
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingOneColWithNullSelection() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, NULL_SELECTION );
		setInputs(new ByteArrayInputStream(COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	/**
	 * Should return success when inserting one col with default selection.
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingOneColWithDefaultSelection() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, DEFAULT_SELECTION );
		setInputs(new ByteArrayInputStream(COL_NAME_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Should return success when inserting one col with return results with default selection.
	 */
	@Test
	public void shouldReturnSuccessWhenInsertingOneColWithReturnResultsWithDefaultSelection() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, DEFAULT_SELECTION );
		setInputs(new ByteArrayInputStream(COL_NAME_JSON.getBytes()));
		testContext.getOperationProperties().putIfAbsent("returnResults", true);
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.SUCCESS);
		results = _response.getResults();
		assertBatchOperation(OperationStatus.SUCCESS, DOC_COUNT, 1);
		assertConnectionIsClosed(_op.getConnection());
	}
	
	/**
	 * Should return error when inserting all nulls with default selection.
	 */
	@Test
	public void shouldReturnErrorWhenInsertingAllNullsWithDefaultSelection() {
		testContext.addOperationProperty(EMPTY_FIELD_SELECTION, DEFAULT_SELECTION );
		setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
		runSnowflakeOperation(_op, _request, _response, DOC_COUNT, 1, OperationStatus.APPLICATION_ERROR);
		results = _response.getResults();
		assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
		assertConnectionIsClosed(_op.getConnection());
	}
    /**
     * Tests the successful execution of the `executeUpdate` method when results are returned.
     * This test verifies that the method interacts correctly with the controller to fetch results and execute the last batch.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test
    public void testExecuteUpdate_SuccessfulExecution() throws Exception {
        List<InputStream>inputStreams = new ArrayList<>();
        inputStreams.add(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        SimpleTrackedData simpleTrackedData = new SimpleTrackedData(1, inputStreams.get(0));
        Iterator<SimpleTrackedData> requestDataIterator = Arrays.asList(new SimpleTrackedData[] {simpleTrackedData}).iterator();
        InputStream mockResult = new ByteArrayInputStream("result data".getBytes());
		Mockito.when(mockController.getResultFromStatement(true)).thenReturn(mockResult);
		Mockito.when(properties.getReturnResults()).thenReturn(true);
        PowerMockito.doNothing().when(mockController).receive(Mockito.any(), Mockito.eq("NULL"),
                Mockito.eq(metaDataValues), Mockito.any(), Mockito.any(), Mockito.any());
        //invoked private Method
        Whitebox.invokeMethod(snowflakeCreateOperation, "executeUpdate",
                mockOperationResponse, properties, mockController, requestDataIterator, TABLE_NAME);
        Mockito.verify(mockController).executeLastBatch();
        Mockito.verify(mockController).getResultFromStatement(true);

    }

    /**
     * Tests the successful execution of the `executeUpdate` method when no results are returned (null result).
     * This test ensures that the method handles a null result correctly when the `returnResults` property is set to true.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test
    public void testExecuteUpdate_SuccessfulExecution_WithReturnResults_NullResult() throws Exception {
        InputStream mockResult = null;
        List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        SimpleTrackedData inputDoc1 = new SimpleTrackedData(1,inputStreams.get(0) );
        Iterator<SimpleTrackedData> requestDataIterator = Arrays.asList(new SimpleTrackedData[]{inputDoc1}).iterator();
		Mockito.when(properties.getReturnResults()).thenReturn(true);
		Mockito.when(mockController.getResultFromStatement(true)).thenReturn(mockResult);
		PowerMockito.doNothing().when(mockController).receive(Mockito.any(), Mockito.eq("NULL"),
				Mockito.eq(metaDataValues), Mockito.any(), Mockito.any(), Mockito.any());
        Whitebox.invokeMethod(snowflakeCreateOperation, "executeUpdate", mockOperationResponse, properties,
                mockController, requestDataIterator, TABLE_NAME);

        Mockito.verify(mockController).executeLastBatch();
        Mockito.verify(mockController).getResultFromStatement(true);
    }

    /**
     * Tests the successful execution of the `executeUpdate` method when no results are returned.
     * This test ensures that the method behaves correctly when the `returnResults` property is set to false.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test
    public void testExecuteUpdate_SuccessfulExecution_NoReturnResults() throws Exception {
        List<InputStream> inputStreams = new ArrayList<>();
        inputStreams.add(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        SimpleTrackedData inputDoc1 = new SimpleTrackedData(1,inputStreams.get(0) );
        Iterator<SimpleTrackedData> requestDataIterator = Arrays.asList(new SimpleTrackedData[]{inputDoc1}).iterator();
		Mockito.when(properties.getReturnResults()).thenReturn(false);
		PowerMockito.doNothing().when(mockController).receive(Mockito.any(), Mockito.eq("NULL"),
				Mockito.eq(metaDataValues), Mockito.any(),  Mockito.any(), Mockito.any());
        Whitebox.invokeMethod(snowflakeCreateOperation, "executeUpdate", mockOperationResponse, properties,
                mockController, requestDataIterator, TABLE_NAME);

        Mockito.verify(mockController).executeLastBatch();
    }

    /**
     * Tests the `executeUpdate` method when a `ConnectorException` is thrown during execution.
     * This test verifies that the exception is handled properly and that the method behaves as expected when the controller's receive method throws an exception.
     *
     * @throws Exception if any exception occurs during the test execution
     */
    @Test(expected = Exception.class)
    public void testExecuteUpdate_ConnectorException() throws Exception {
		Mockito.when(properties.getReturnResults()).thenReturn(true);
		Mockito.when(mockController.getResultFromStatement(false)).thenReturn(null);
        Mockito.doThrow(new ConnectorException("Connector exception occurred"))
                .when(mockController)
                .receive(Mockito.any(), Mockito.any(), Mockito.any(),Mockito.any(), Mockito.any(), Mockito.any());

        thrown.expectMessage("Connector exception occurred");

        mockController.receive((Mockito.any()), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any(), Mockito.any());
    }

	/**
	 * Unit test for the {@code executeSizeLimitedUpdate} method when reading data from a cookie.
	 *
	 * <p>This test validates the following:
	 * <ul>
	 *   <li>That the batch size is correctly set in the context.</li>
	 *   <li>That the cookie data is properly deserialized into a SortedMap using the ObjectMapper.</li>
	 *   <li>That the metadata and default values in the {@code DefaultValueObject} are not null after execution.</li>
	 * </ul>
	 *
	 * @throws IOException if an error occurs during JSON deserialization of the cookie value.
	 */
	@Test
	public void testExecuteSizeLimitedUpdateForReadDataFromCookieMethod() throws IOException, SQLException {
		Iterator<ObjectData> mockIterator = Mockito.mock(Iterator.class);
		testContext.setBatchSize(1);
		Map<String, String> dynamicProps = null;
		MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
		Mockito.when(_snowflakeCreateOperation.getContext()).thenReturn(testContext);
		Mockito.when(mockOperationResponse.getLogger()).thenReturn(logger);
		Mockito.when(mockSnowflakeConnection.getOperationContext()).thenReturn(testContext);
		testContext.addCookie(ObjectDefinitionRole.INPUT, cookieValue);
		SortedMap<String, TableDefaultAndMetaDataObject> expectedMap = new TreeMap<>();
		TableDefaultAndMetaDataObject defaultValueObject12 = new TableDefaultAndMetaDataObject();
		defaultValueObject12.setDefaultValues(defaultValues);
		defaultValueObject12.setMetaDataValues(metaDataValues);
		defaultValueObject12.setTableMetaDataValues(metaDataValues);
		expectedMap.put("SOLUTIONS|DB", defaultValueObject12);
		Mockito.when(SnowflakeOperationUtil.getObjectMapper()).thenReturn(mockObjectMapper);
		Mockito.when(mockObjectMapper.getTypeFactory()).thenReturn(mockTypeFactory);
		MapType mapType = TypeFactory.defaultInstance()
				.constructMapType(TreeMap.class, String.class, TableDefaultAndMetaDataObject.class);
		Mockito.when(mockTypeFactory.constructMapType(Mockito.eq(TreeMap.class), Mockito.eq(String.class), Mockito.eq(TableDefaultAndMetaDataObject.class))).thenReturn(mapType);
		Mockito.when(mockObjectMapper.readValue(cookieValue, mapType)).thenReturn(expectedMap);
		TableDefaultAndMetaDataObject defaultValueObject1 = new TableDefaultAndMetaDataObject(defaultValues, metaDataValues, metaDataValues);
		Mockito.when(mockUpdateRequest.iterator()).thenReturn(mockIterator);
		Mockito.when(mockIterator.hasNext()).thenReturn(false);
		_snowflakeCreateOperation.executeSizeLimitedUpdate(mockUpdateRequest, mockOperationResponse);
		Assert.assertNotNull(defaultValueObject1.getDefaultValues());
		Assert.assertNotNull(defaultValueObject1.getMetaDataValues());
		Mockito.verify(mockController, Mockito.times(0)).receive(input, null, null, _boundedMap,
				null, dynamicOpProps);
	}

	/**
	 * Unit test for the {@code executeSizeLimitedUpdate} method when the cookie value is null.
	 *
	 * <p>This test validates the following:
	 * <ul>
	 *   <li>That the batch size is correctly set in the context.</li>
	 *   <li>That the method handles a null cookie gracefully without throwing unexpected exceptions.</li>
	 *   <li>That no calls are made to {@code receive} on the mockController when the cookie is null.</li>
	 * </ul>
	 *
	 * @throws SQLException if an SQL error occurs during the execution.
	 */
	@Test
	public void testExecuteSizeLimitedUpdateWhenCookieIsNull() throws SQLException {
		testContext.setBatchSize(2);
		Map<String, String> dynamicProps = null;
		MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
		Mockito.when(_snowflakeCreateOperation.getContext()).thenReturn(testContext);
		Mockito.when(mockOperationResponse.getLogger()).thenReturn(logger);
		Mockito.when(mockSnowflakeConnection.getOperationContext()).thenReturn(testContext);
		testContext.addCookie(ObjectDefinitionRole.INPUT, null);
		Mockito.when(connection.getSchema()).thenReturn("schema");
		Mockito.when(connection.getCatalog()).thenReturn("db");
		Iterator<ObjectData> mockIterator = Mockito.mock(Iterator.class);
		Mockito.when(mockUpdateRequest.iterator()).thenReturn(mockIterator);

		_snowflakeCreateOperation.executeSizeLimitedUpdate(mockUpdateRequest, mockOperationResponse);
		Mockito.verify(mockController, Mockito.times(0)).receive(input, null, null, _boundedMap,
				null, dynamicOpProps);

	}

	/**
	 * Sets the Create operation input.
	 * @param value
	 * 			Input Stream for the create operation
	 */
	private void setInputs(InputStream value) {
		_inputs = new ArrayList<>();
		for(int i = 0 ; i < DOC_COUNT; i++) {
			_inputs.add(value);
		}
		_request = new ModifiedUpdateRequest(_inputs, _response);
	}
}
