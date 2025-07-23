// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.get;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.util.QueryBuilderUtil;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;

import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestConstants;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.util.BaseConnection;
import com.boomi.connector.util.ContextualOperation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
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
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;


public class DynamicGetOperationTest {

    private static final String OBJECT_TYPE_ID = "EVENT";
    private static final String OBJECT_TYPE_ID_FOR_VIEW = "VIEW_TABLE";
    private static final String JSON_STRING = "{\n" + "\"documentBatching\": true\n" + "  \n" + "}\n";
    private static final String INPUT = "{\"Name\":\"abc\",\"id\":2,\"date\":\"2023-03-29\"}";

    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final PreparedStatement _preparedStatement2 = Mockito.mock(PreparedStatement.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final OperationContext _operationContext = Mockito.mock(OperationContext.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSet2 = Mockito.mock(ResultSet.class);
    private final ResultSetMetaData _resultSetMetaData = Mockito.mock(ResultSetMetaData.class);
    private final DynamicGetOperation dynamicGetOperation = new DynamicGetOperation(_databaseConnectorConnection);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final Map<String, String> dynamicProps = new HashMap<>();
    private ContextualOperation _contextualOperation = Mockito.mock(ContextualOperation.class);
    private BaseConnection _baseConnection = Mockito.mock(BaseConnection.class);

    @Before
    public void setup() {
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
    }

    /**
     * Test to check if result is success for Object_Type_Id
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteGetOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test to check if payload and result is success for Object_Type_Id
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteSuccessByStatusCodeAndMessageGetOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        String operationStatusCode = simpleOperationResponse.getResults().get(0).getStatusCode();
        String operationStatusMessage = simpleOperationResponse.getResults().get(0).getMessage();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, operationStatusCode);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, operationStatusMessage);
    }

    /**
     * Test to check if payload is success for Object_Type_Id
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteSuccessBypayloadGetOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<byte[]> operationStatus=simpleOperationResponse.getResults().get(0).getPayloads();
        String payload=new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"[]", payload);
    }

    /**
     * Test to check if payload and result is success for Object_Type_Id for multiple documents payload
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testSucccessforGetMultipleDocsbyid() throws SQLException {
        setupDataForExecuteOperationMultidocs(OBJECT_TYPE_ID);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_resultSet2.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

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
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"[]",new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Test to check if payload and result is success for Object_Type_Id_for_view
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteGetViewsOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID_FOR_VIEW);
        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test to check if payload and result is success for Object_Type_Id_for_view
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteSuccessByStatusCodeAndMessageGetViewOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID_FOR_VIEW);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        String operationStatusCode = simpleOperationResponse.getResults().get(0).getStatusCode();
        String operationStatusMessage = simpleOperationResponse.getResults().get(0).getMessage();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE, operationStatusCode);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_MESSAGE,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE, operationStatusMessage);
    }

    /**
     * Test to check if payload is success for Object_Type_Id_for_view
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteSuccessBypayloadGetViewOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID_FOR_VIEW);
        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        String payload=new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"[]", payload);
    }

    /**
     * Test to check if payload and result is success for Object_Type_Id_for_view for multiple document and payloads
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testSucccessforGetMultipleDocsbyidview() throws SQLException {
        setupDataForExecuteOperationMultidocs(OBJECT_TYPE_ID_FOR_VIEW);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_resultSet2.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

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
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"[]",new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Test to check if payload and result is failure for Object_Type_Id
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteFailureByStatusCodeAndMessageGetOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, "",simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Connection failed, please check connection details!",simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResponse.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for Object_Type_Id_for_view
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteFailureByStatusCodeAndMessageGetViewOperation() throws SQLException {
        setupDataForExecuteOperation(OBJECT_TYPE_ID_FOR_VIEW);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "",simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Connection failed, please check connection details!",simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResponse.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for Object_Type_Id_for_view for multiple documents failure
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testFailureforGetMultipleDocsbyid() throws SQLException {
        setupDataForExecuteOperationMultidocs(OBJECT_TYPE_ID);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_resultSet2.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));

        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResult.getStatus());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, "",simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Connection failed, please check connection details!",simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,simpleOperationResult.getPayloads().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        }
    }

    /**
     * Test to check if payload and result is failure for Object_Type_Id_for_view for multiple documents failure
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testFailureforGetMultipleDocsbyidview() throws SQLException {
        setupDataForExecuteOperationMultidocs(OBJECT_TYPE_ID_FOR_VIEW);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_resultSet2.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(null);

        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setUpResultSetForVarChar(_resultSet);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.FAILURE, simpleOperationResult.getStatus());
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE_STATUS_CODE, "",simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Connection failed, please check connection details!",simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,simpleOperationResult.getPayloads().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        }
    }


    @Test
    public void testExecuteGetInClauseOperation() throws SQLException {

        Mockito.when(_contextualOperation.getConnection()).thenReturn(_baseConnection);
        Mockito.when(_baseConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_propertyMap.getBooleanProperty(DatabaseConnectorConstants.IN_CLAUSE, false)).thenReturn(true);

        setupDataForExecuteOperation(OBJECT_TYPE_ID_FOR_VIEW);
        dynamicProps.clear();
        dynamicProps.putIfAbsent("param_where", "SUP_ID = $SUP_ID AND Name = $Name AND SUP_ID IN($SUP_ID)");
        dynamicProps.putIfAbsent("param_orderBy", "SUP_ID DESC");

        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false)
                .thenReturn(true).thenReturn(false)
                .thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
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
        Mockito.when(_contextualOperation.getConnection()).thenReturn(_baseConnection);
        Mockito.when(_baseConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID_FOR_VIEW);
        setupDataForExecuteGetOrGetViewOperation();
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_FOR_VIEW, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        String input =
                "{\"price\":999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000}";
        dynamicProps.clear();
        Map<String, String> map = new HashMap<>();
        map.put("param_where", "price = $price");
        setupTrackedData(input, map);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true)
                .thenReturn(true).thenReturn(true).thenReturn(false);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(
                        "999999999999999999999999999999999999990000000000000000000000000000000000000000000000000000000000000000000000000000000000000000"),
                actual);
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteDoubleBigValueDecimals() throws SQLException {
        Mockito.when(_contextualOperation.getConnection()).thenReturn(_baseConnection);
        Mockito.when(_baseConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID_FOR_VIEW);
        setupDataForExecuteGetOrGetViewOperation();
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_FOR_VIEW, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        String input = "{\"price\":12345678901234567890123456789012345.12345678901234567890123456789}";
        dynamicProps.clear();
        Map<String, String> map = new HashMap<>();
        map.put("param_where", "price = $price");
        setupTrackedData(input, map);
        DataTypesUtil.setUpResultSetNumericDouble(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true)
                .thenReturn(true).thenReturn(true).thenReturn(false);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal("12345678901234567890123456789012345.12345678901234567890123456789"), actual);
    }

    /**
     * Test Numeric data type precision for Oracle DB
     *
     * @throws SQLException
     */
    @Test
    public void testExecuteIntegerBigValue() throws SQLException {
        Mockito.when(_contextualOperation.getConnection()).thenReturn(_baseConnection);
        Mockito.when(_baseConnection.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID_FOR_VIEW);
        setupDataForExecuteGetOrGetViewOperation();
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID_FOR_VIEW, null)).thenReturn(_resultSet);
        Mockito.when(_connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(_preparedStatement);
        String input =
                "{\"price\":99999999999999999999999999999999999999}";
        dynamicProps.clear();
        Map<String, String> map = new HashMap<>();
        map.put("param_where", "price = $price");
        setupTrackedData(input, map);
        DataTypesUtil.setUpResultSetNumericInteger(_resultSet);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false).thenReturn(true)
                .thenReturn(true).thenReturn(true).thenReturn(false);

        dynamicGetOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);

        ArgumentCaptor<BigDecimal> argumentCaptor = ArgumentCaptor.forClass(BigDecimal.class);
        Mockito.verify(_preparedStatement).setBigDecimal(ArgumentMatchers.anyInt(), argumentCaptor.capture());
        BigDecimal actual = argumentCaptor.getValue();
        Assert.assertEquals(new BigDecimal(
                        "99999999999999999999999999999999999999"),
                actual);
    }

    public void setupDataForExecuteGetOrGetViewOperation() throws SQLException {
        Mockito.when(dynamicGetOperation.getContext()).thenReturn(_operationContext);
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

        for(Map.Entry <String, String> entry : dynamicProp.entrySet())
        {
            dynamicProps.putIfAbsent(entry.getKey(),entry.getValue());
        }
        SimpleTrackedData trackedData = new SimpleTrackedData(1, result, null, dynamicProps);

        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true, false);
        Mockito.when(objDataItr.next()).thenReturn(trackedData);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
    }

    private void setupTrackedDataMultidocs(String input, Map<String, String> dynamicProp) {
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        for(Map.Entry <String, String> entry : dynamicProp.entrySet())
        {
            dynamicProps.putIfAbsent(entry.getKey(),entry.getValue());
        }
        SimpleTrackedData trackedData = new SimpleTrackedData(1, result, null, dynamicProps);
        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(objDataItr.next()).thenReturn(trackedData);
        SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
    }


    private void setupDataForExecuteOperation(String objectTypeIdForView) throws SQLException {
        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);

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

    private void setupDataForExecuteOperationMultidocs(String objectTypeIdForView) throws SQLException {
        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(objectTypeIdForView);
        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(false);
        setupDataForExecuteGetOrGetViewOperation();
        Mockito.when(_databaseMetaData.getColumns(null, null, objectTypeIdForView, null)).thenReturn(_resultSet);
        HashMap<String, String> dynamicProp = new HashMap<>();
        dynamicProp.put("param_where", "SUP_ID = $SUP_ID AND SUP_NAME = $SUP_NAME");
        dynamicProp.put("param_orderBy", "SUP_ID DESC");
        setupTrackedDataMultidocs(INPUT, dynamicProp);
    }

}