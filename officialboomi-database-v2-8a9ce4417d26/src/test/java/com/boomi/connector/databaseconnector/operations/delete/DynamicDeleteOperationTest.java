// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.operations.delete;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.DatabaseConnectorTestConstants;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResponseWrapper;
import com.boomi.connector.testutil.SimpleOperationResult;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DynamicDeleteOperationTest {

    private static final String COLUMN_NAME_NAME = "customerId";
    private static final String OBJECT_TYPE_ID = "customerTest";
    private static final String COMMIT_BY_PROFILE = "Commit By Profile";
    private static final String INPUT = "{\"WHERE\":[{\"column\":\"customerId\",\"value\":\"111111\",\"operator\":\"=\"}]}";
    private static final String ERROR_MESSAGE = "Response status is not SUCCESS";
    private static final String ASSERT_DOC_COUNT = "Assert that there are 2 docs";
    private final OperationContext _operationContext = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final DynamicDeleteOperation _dynamicDeleteOperation = new DynamicDeleteOperation(_databaseConnectorConnection);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final DatabaseMetaData _databaseMetaDataForValidate = Mockito.mock(DatabaseMetaData.class);
    private final SimpleOperationResponse _simpleOperationResponse = new SimpleOperationResponse();
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final ResultSet _resultSetColumnNameName = Mockito.mock(ResultSet.class);

    @Before
    public void setup() throws SQLException {

        Mockito.when(_dynamicDeleteOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);

        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);

        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        Mockito.when(_propertyMap.get(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");

        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);

    }

    @Test
    public void executeForDeleteByCommitRowBatchEquals() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        setupTrackedData();

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);

    }

    @Test
    public void executeForDeleteByCommitRowBatchNotEquals() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(2L);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        setupTrackedData();

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);

    }

    /**
     * Test to check correctness of response status and payload in case of successful commit by Profile Dynamic Delete
     *
     * @throws SQLException
     */
    @Test
    public void testDynamicDeleteCommitByProfileResponseForSuccess() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(COMMIT_BY_PROFILE);

        setupTrackedData();

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Ok", operationMessage);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                "{\"Query\":\"DELETE FROM customerTest WHERE customerId=?\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check correctness of response status and payload in case of successful commit by rows Dynamic Delete
     *
     * @throws SQLException
     */
    @Test
    public void testDynamicDeleteCommitByRowResponseForSuccesss() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);

        setupTrackedData();

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Ok", operationMessage);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}",
                new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
    }

    /**
     * Test to check if payload and result is correct for multiple docs
     *
     * @throws SQLException
     */
    @Test
    public void testDynamicDeletePayloadSuccessMultiDocs()
            throws SQLException {
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        Assert.assertEquals(ASSERT_DOC_COUNT, 2, results.size());

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE,
                    simpleOperationResult.getMessage());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE,
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    "{\"Query\":\"DELETE FROM customerTest WHERE customerId=?\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        }
    }

    /**
     * Test to check correctness of response status and payload in case of unsuccessful Dynamic Delete
     *
     * @throws SQLException
     */
    @Test
    public void testDynamicDeleteResponseForFailure() throws SQLException {

        setupTrackedData();
        DataTypesUtil.setupInput(INPUT, _updateRequest, _simpleOperationResponse);
        DataTypesUtil.setUpResultSetForIntegerForException(_resultSet);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();
        String operationMessage = _simpleOperationResponse.getResults().get(0).getMessage();
        String operationStatusCode = _simpleOperationResponse.getResults().get(0).getStatusCode();
        SimpleOperationResult simpleOperationResult = _simpleOperationResponse.getResults().get(0);

        Assert.assertEquals("Response Status is not FAILURE", OperationStatus.FAILURE, operationStatus);
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "", operationStatusCode);
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException", operationMessage);
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResult.getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is correct for multiple docs
     *
     * @throws SQLException
     */
    @Test
    public void testDynamicDeletePayloadFailureMultiDocs()
            throws SQLException {
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        setupMultipleTrackedData(inputs, _updateRequest, _simpleOperationResponse);

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        List<SimpleOperationResult> results = _simpleOperationResponse.getResults();
        Assert.assertEquals(ASSERT_DOC_COUNT, 2, results.size());

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals("Response status is not FAILURE", OperationStatus.FAILURE, simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "", simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException", simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED, simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED, simpleOperationResult.getPayloads().isEmpty());
        }
    }

    @Test
    public void executeForDeleteByProfileRow() throws SQLException {

        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_databaseMetaDataForValidate.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.getProperty(Mockito.anyString())).thenReturn(COMMIT_BY_PROFILE);

        setupTrackedData();

        _dynamicDeleteOperation.executeSizeLimitedUpdate(_updateRequest, _simpleOperationResponse);
        OperationStatus operationStatus = _simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);

    }

    private void setupTrackedData() throws SQLException {

        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        SimpleTrackedData trackedData = new SimpleTrackedData(13, result);
        Iterator<ObjectData> objDataItr = Mockito.mock(Iterator.class);

        Mockito.when(_updateRequest.iterator()).thenReturn(objDataItr);
        Mockito.when(objDataItr.hasNext()).thenReturn(true, false);
        Mockito.when(objDataItr.next()).thenReturn(trackedData);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, COLUMN_NAME_NAME))
                .thenReturn(_resultSetColumnNameName);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(false);

        SimpleOperationResponseWrapper.addTrackedData(trackedData, _simpleOperationResponse);
    }

    /**
     * Mocks update request to return multiple docs.
     * @param result
     * @param updateRequest
     * @param simpleOperationResponse
     */
    public void setupMultipleTrackedData(List<InputStream> result, UpdateRequest updateRequest,
            SimpleOperationResponse simpleOperationResponse) throws SQLException {
        List<ObjectData> trackedDataList = new ArrayList<>();
        for (int i = 0; i < result.size(); i++) {
            SimpleTrackedData trackedData = new SimpleTrackedData(i, result.get(i));
            trackedDataList.add(trackedData);
            SimpleOperationResponseWrapper.addTrackedData(trackedData, simpleOperationResponse);
        }
        Iterator<ObjectData> objDataItr = trackedDataList.listIterator();
        Mockito.when(updateRequest.iterator()).thenReturn(objDataItr);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null))
                .thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, COLUMN_NAME_NAME))
                .thenReturn(_resultSetColumnNameName);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn("TABLE");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(true).thenReturn(false);
    }
}