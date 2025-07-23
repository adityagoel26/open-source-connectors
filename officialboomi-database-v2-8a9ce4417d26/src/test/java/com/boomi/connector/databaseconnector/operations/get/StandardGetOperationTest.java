// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.operations.get;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.api.ui.DataType;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestConstants;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleTrackedData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Test {@link StandardGetOperation} class
 */
public class StandardGetOperationTest {

    private static final String INCLAUSE_QUERY_STRING = "SELECT * FROM Test.EVENT WHERE USER_ID IN ($USER_ID)";
    private static final String INCLAUSE_QUERY_DATE = "SELECT * FROM EMP_TABLE WHERE DATE IN ($DATE)";
    private static final String SELECT_QUERY = "SELECT * FROM Test.EVENT WHERE USER_ID = $USER_ID";
    private static final String OBJECT_TYPE_ID = "EVENT";
    private static final String OBJECT_TYPE_ID_DATE = "EMP_TABLE";
    private static final String USER_ID_COLUMN_NAME = "USER_ID";
    private static final String DATABASE_PRODUCT_NAME = "testSchema";
    private static final String INPUT = "{\"USER_ID\":\"123\"}";
    private static final String INPUT2 = "{\"USER_ID\":\"120\"}";
    private static final String INPUTDATE = "{\"DATE\":\"2022-03-03\"}";
    private static final String INPUTDATES = "{\"DATE\" : [\"2022-03-03\", \"2021-04-03\", \"2019-03-12\"]}";
    private static final String INPUTJSON = "{\"USER_ID\":{\"id\":\"123\"}}";
    private static final String JSON_STRING             = "{\n" + "\"documentBatching\": true\n" + "  \n" + "}\n";
    private final List<ObjectType> _objectTypes = Mockito.mock(ArrayList.class);
    private final PropertyMap _operationPropertiesMap = Mockito.mock(PropertyMap.class);
    private final Iterator<ObjectData> _objDataItr = Mockito.mock(Iterator.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSet2 = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetForMetaData = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final PreparedStatement _preparedStatement2 = Mockito.mock(PreparedStatement.class);
    private final UpdateRequest               _updateRequest               = Mockito.mock(UpdateRequest.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final PropertyMap                 _propertyMap                 = Mockito.mock(PropertyMap.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final OperationContext _operationContext =Mockito.mock(OperationContext.class);
    private final StandardGetOperation standardGetOperation = new StandardGetOperation(_databaseConnectorConnection);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final Map<String, String> dynamicProps = new HashMap<>();

    /**
     * Setup the document
     */
    @Before
    public void setup() {

        Mockito.when(_updateRequest.iterator()).thenReturn(_objDataItr);
        Mockito.when(_objDataItr.hasNext()).thenReturn(true, false);
    }

    /**
     * Assert the connection
     */
    @Test
    public void testDatabaseConnection() {
        Assert.assertNotNull(_databaseConnectorConnection);
    }

    /**
     * Test standard update errored
     * @throws SQLException
     */
    @Test
    public void testExecuteForInClauseIsChecked() throws SQLException {
        String expectedErrorMessageForInClause =
                "Kindly select the IN clause check box only if Sql Query contains IN Clause !";
        Mockito.when(_operationPropertiesMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE)).thenReturn(false);

        setupDataForExecuteGetOperation();
        setStringInputContext();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        String actualErrorMessage = simpleOperationResponse.getResults().get(0).getMessage();
        Assert.assertEquals(expectedErrorMessageForInClause, actualErrorMessage);
    }

    /**
     * Test standard update success
     * @throws SQLException
     */
    @Test
    public void testExecuteForInClauseSuccessOperation() throws SQLException {
        setupDataForExecuteGetOperation();
        setStringInputContext();
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update success and payloads
     * @throws SQLException
     */
    @Test
    public void testExecuteForStatusCodeAndSuccessMessageStandardGet() throws SQLException{
        setupDataForExecuteGetOperation();
        setStringInputContext();
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);

        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        String operationStatusCode = simpleOperationResponse.getResults().get(0).getStatusCode();
        String operationMessage = simpleOperationResponse.getResults().get(0).getMessage();
        String payload=new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, operationStatusCode);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, operationMessage);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE,"{\"LABEL\":null}", payload);
    }

    /**
     * Test standard update success for multiple documents
     * @throws SQLException
     */
    @Test
    public void testExecuteForInClauseSuccessOperationMulti() throws SQLException{
        setupDataForExecuteGetOperation();
        setStringInputContext();
        setContextForINClause();

        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetForMetaData.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT2.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE, DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE,"{\"LABEL\":null}",new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Test standard update failure
     * @throws SQLException
     */
    @Test
    public void testExecuteForFailureStandardGet() throws SQLException{
        setupDataForExecuteGetOperation();
        setStringInputContext();
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);

        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, "",simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Connection failed, please check connection details!",simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,simpleOperationResponse.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test standard update failure for payload and multiple documents
     * @throws SQLException
     */
    @Test
    public void testExecuteForInClauseFailureOperationMulti() throws SQLException{
        setupDataForExecuteGetOperation();
        setStringInputContext();
        setContextForINClause();

        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_STRING);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetForMetaData.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT2.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResult.getStatus());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, "",simpleOperationResult.getStatusCode());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE, "Connection failed, please check connection details!",simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,simpleOperationResult.getPayloads().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,simpleOperationResult.getPayloadMetadatas().isEmpty());
        }
    }

    /**
     * Test standard update for date
     * @throws SQLException
     */
    @Test
    public void testExecuteMysqlInClauseOperationForDate() throws SQLException {
        setupDataForExecuteGetOperation();
        setDateInputContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_DATE);
        DataTypesUtil.setupInput(INPUTDATE, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update for date
     * @throws SQLException
     */
    @Test
    public void testExecuteInClauseOperationForDate() throws SQLException {
        setupDataForExecuteGetOperation();
        setDateInputContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DATABASE_PRODUCT_NAME);
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_DATE);

        DataTypesUtil.setupInput(INPUTDATE, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update with string array dates
     * @throws SQLException
     */
    @Test
    public void testExecuteInClauseOperationForStringArrayDates() throws SQLException {
        setupDataForExecuteGetOperation();
        setDateInputContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_DATE);

        DataTypesUtil.setupInput(INPUTDATES, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update for node array dates
     * @throws SQLException
     */
    @Test
    public void testExecuteInClauseOperationForNodeArrayDates() throws SQLException {
        setupDataForExecuteGetOperation();
        setDateInputContext();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DATABASE_PRODUCT_NAME);
        setContextForINClause();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(),ArgumentMatchers.anyString())).thenReturn(
                INCLAUSE_QUERY_DATE);

        DataTypesUtil.setupInput(INPUTDATES, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update with user data
     * @throws SQLException
     */
    @Test
    public void testExecuteSuccessForUserData() throws SQLException {
        setupDataForExecuteGetOperation();
        setStringInputContext();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(SELECT_QUERY);
        DataTypesUtil.setupInput(INPUT, _updateRequest, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update with JSON user data
     * @throws SQLException
     */
    @Test
    public void testExecuteSuccessForUserDataJson() throws SQLException {
        setupDataForExecuteGetOperation();
        setStringInputContext();

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.ORACLE);
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(SELECT_QUERY);

        DataTypesUtil.setupInput(INPUTJSON, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetJson(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(USER_ID_COLUMN_NAME);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test standard update
     * @throws SQLException
     */
    @Test
    public void testExecuteSuccessForNoUserData() throws SQLException {
        String noInput = "";
        setupDataForExecuteGetOperation();
        setStringInputContext();
        Mockito.when(_operationContext.getOperationProperties().getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(
                "SELECT * FROM Test.EVENT");
        Mockito.when(_resultSet.next()).thenReturn(false);
        SimpleTrackedData trackedData = new SimpleTrackedData(11, null);
        Mockito.when(_objDataItr.next()).thenReturn(trackedData);
        DataTypesUtil.setupInput(noInput, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetBoolean(_resultSet);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValue() throws SQLException {
        testExecuteBigValue(
                "{\"price\":999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}",
                "999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000");
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValueDecimals() throws SQLException {
        testExecuteBigValue("{\"price\":12345678901234567890123456789012345.12345678901234567890123456789}",
                "12345678901234567890123456789012345.12345678901234567890123456789");
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteIntegerBigValue() throws SQLException {
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false)).thenReturn(true);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(
                "SELECT * FROM EVENT WHERE price IN ( $price )");
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        String input =
                "{\"price\":99999999999999999999999999999999999999}";
        setupTrackedData(input, new HashMap<>());
        DataTypesUtil.setUpResultSetNumericInteger(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true)
                .thenReturn(true).thenReturn(true).thenReturn(false);

        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(
                        "99999999999999999999999999999999999999"),
                actual);
    }

    /**
     * Test case to verify the execution of a SQL query with a single NVARCHAR parameter.
     *
     * <p>This test verifies that when executing a SQL query with a single NVARCHAR parameter,
     * the parameter value is properly set on the prepared statement.</p>
     *
     * @throws SQLException if a database access error occurs
     */
    @Test
    public void testExecuteNvarcharValueEquals() throws SQLException {
        // Mock setup
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(
                "SELECT * FROM EVENT WHERE id = ( $id )");
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);

        // Test data
        String input = "{\"id\":\"AnyStringValue\"}";
        setupTrackedData(input, new HashMap<>());
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        // Behavior setup
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);

        // Method invocation
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        // Verification
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(_preparedStatement).setNString(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        String actual = argumentCaptor.getValue();
        Assert.assertEquals("AnyStringValue", actual);
    }

    /**
     * Tests the execution of an SQL query with a VARCHAR parameter in the IN clause.
     * Verifies that the correct value is set to the PreparedStatement.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed result set
     */
    @Test
    public void testExecuteNvarcharValueIn() throws SQLException {
        // Mock setup
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false)).thenReturn(true);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(
                "SELECT * FROM EVENT WHERE id IN ( $id )");
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);

        // Test data
        String input = "{\"id\":\"AnyLargeStringValue\"}";
        setupTrackedData(input, new HashMap<>());
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        // Behavior setup
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);

        // Method invocation
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        // Verification
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(_preparedStatement).setNString(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        String actual = argumentCaptor.getValue();
        Assert.assertEquals("AnyLargeStringValue", actual);
    }

    /**
     * Tests the execution of an SQL query with a VARCHAR parameter in the IN clause.
     * Verifies that the correct value is set to the PreparedStatement.
     *
     * @throws SQLException if a database access error occurs or this method is called on a closed result set
     */
    @Test
    public void testExecuteNvarcharValueWithQuotes() throws SQLException {
        // Mock setup
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(
                "SELECT * FROM EVENT WHERE id = ( $id )");
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);

        // Test data
        String input = "{\"id\":\"NOCOMMENTS\\\"COMMENTS\\\"NOCOMMENTS\"}";
        setupTrackedData(input, new HashMap<>());
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        // Behavior setup
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);

        // Method invocation
        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        // Verification
        ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(_preparedStatement).setNString(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        String actual = argumentCaptor.getValue();
        Assert.assertEquals("NOCOMMENTS\"COMMENTS\"NOCOMMENTS", actual);
    }

    private void testExecuteBigValue(String input, String expected) throws SQLException {
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false)).thenReturn(true);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.QUERY, "")).thenReturn(
                "SELECT * FROM EVENT WHERE price IN ( $price )");
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        setupTrackedData(input, new HashMap<>());
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false)
                .thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);

        standardGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(expected), actual);
    }

    private void setupDataForExecuteGetOrGetViewOperation() throws SQLException {
        Mockito.when(standardGetOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);

        Mockito.when(_propertyMap.getLongProperty(ArgumentMatchers.anyString())).thenReturn((long) 1);
        Mockito.when(_propertyMap.get(ArgumentMatchers.anyString())).thenReturn("Schema Name");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");

        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement2);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn("Microsoft SQL Server");

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("Name");
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.getMetaData()).thenReturn(_resultSetMetaData);
        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(0);
        Mockito.when(_resultSet2.next()).thenReturn(true).thenReturn(false);

        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
        Mockito.when(_preparedStatement2.executeQuery()).thenReturn(_resultSet2);
        Mockito.when(_operationContext.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn(JSON_STRING);
    }

    private void setupTrackedData(String input, Map<String, String> dynamicProp) {
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));

        for (Map.Entry<String, String> entry : dynamicProp.entrySet()) {
            dynamicProps.putIfAbsent(entry.getKey(), entry.getValue());
        }
        SimpleTrackedData trackedData = new SimpleTrackedData(1, result, null, dynamicProps);

        Iterator<ObjectData> objDataItr =  Mockito.mock(Iterator.class);
        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true, false);
        Mockito.when(objDataItr.next()).thenReturn(trackedData);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
    }

    private void setupDataForExecuteOperation(String objectTypeIdForView) throws SQLException {
        Iterator<ObjectData> objDataItr =  Mockito.mock(Iterator.class);

        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(objectTypeIdForView);
        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true, false);

        setupDataForExecuteGetOrGetViewOperation();
        Mockito.when(_databaseMetaData.getColumns(null, null, objectTypeIdForView, null)).thenReturn(_resultSet);

        HashMap<String, String> dynamicProp = new HashMap<>();
        dynamicProp.put("param_where", "SUP_ID = $SUP_ID AND SUP_NAME = $SUP_NAME");
        dynamicProp.put("param_orderBy", "SUP_ID DESC");
        setupTrackedData(INPUT, dynamicProp);
    }

    private void setContextForINClause() {
        Mockito.when(_operationPropertiesMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE)).thenReturn(true);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_operationPropertiesMap);
        Mockito.when(_operationContext.getOperationProperties().getBooleanProperty(ArgumentMatchers.anyString(), ArgumentMatchers.any())).thenReturn(true);
    }

    private void setStringInputContext() throws SQLException {
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DATABASE_PRODUCT_NAME);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(USER_ID_COLUMN_NAME);

        Mockito.when(_resultSetForMetaData.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSetForMetaData.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(USER_ID_COLUMN_NAME);
    }

    private void setDateInputContext() throws SQLException {
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID_DATE);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_DATE, null)).thenReturn(_resultSet);

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("91");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("DATE");

        Mockito.when(_resultSetForMetaData.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("91");
        Mockito.when(_resultSetForMetaData.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn("DATE");
    }

    private void setupDataForExecuteGetOperation() throws SQLException {

        Mockito.when(standardGetOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getProperty(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn("Testing");
        Mockito.when(_propertyMap.getLongProperty(ArgumentMatchers.anyString())).thenReturn(10L);
        Mockito.when(_propertyMap.getProperty(ArgumentMatchers.anyString())).thenReturn("Schema Name");
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSetForMetaData);

        Mockito.when(_objectTypes.size()).thenReturn(2);
        Mockito.when(_operationPropertiesMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_operationPropertiesMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");
        Mockito.when(_operationPropertiesMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE)).thenReturn(false);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_operationPropertiesMap);

        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("TABLE");
        Mockito.when(_resultSet.getShort(5)).thenReturn((short) 4);
        Mockito.when(_resultSet.getString(4)).thenReturn("RETURN_VALUES");

        Mockito.when(_databaseMetaData.getProcedureColumns("test", _connection.getSchema(), "Test", null)).thenReturn(
                _resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false);

        Mockito.when(_resultSetForMetaData.getString(1)).thenReturn("COLUMN");
        Mockito.when(_resultSetForMetaData.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("TABLE");
        Mockito.when(_resultSetForMetaData.getShort(5)).thenReturn((short) 4);
        Mockito.when(_resultSetForMetaData.getString(4)).thenReturn("RETURN_VALUES");
        Mockito.when(_resultSetForMetaData.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetForMetaData.getMetaData()).thenReturn(_resultSetMetaData);

        Mockito.when(_resultSetMetaData.getColumnCount()).thenReturn(1);
        Mockito.when(_resultSetMetaData.getColumnType(1)).thenReturn(12);
        Mockito.when(_resultSetMetaData.getColumnLabel(1)).thenReturn("LABEL");
    }

}
