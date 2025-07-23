// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.update;

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
import com.boomi.connector.testutil.SimpleOperationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class DynamicUpdateTest {

    private static final String OBJECT_TYPE_ID = "CUSTOMER";
    private final static String COLUMN_NAME_NAME = "name";
    private final static String COLUMN_NAME_ID = "id";
    private static final String COMMIT_BY_ROWS = "Commit By Rows";
    private static final String COMMIT_BY_PROFILE = "Commit By Profile";

    private final OperationContext _operationContext = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final Connection _connection = Mockito.mock(Connection.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetColumnNameName = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetColumnNameId = Mockito.mock(ResultSet.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);

    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();
    private final DynamicUpdateOperation dynamicUpdateOperation = new DynamicUpdateOperation(
            _databaseConnectorConnection);

    @Before
    public void setup() throws SQLException, IOException {
        String INPUT = new String(Files.readAllBytes(Paths.get("src/test/resource/DynamicUpdateTest.txt")));

        Mockito.when(dynamicUpdateOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);

        Mockito.when(_connection.prepareStatement(Matchers.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareStatement(Matchers.anyString(), Matchers.anyInt())).thenReturn(_preparedStatement);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);

        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);

        Mockito.when(_propertyMap.getLongProperty(Matchers.anyString())).thenReturn(1L);
        Mockito.when(_propertyMap.get(Matchers.anyString())).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.GET_TYPE)).thenReturn("get");
        Mockito.when(_propertyMap.get(DatabaseConnectorConstants.TYPE)).thenReturn("type");

        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, null)).
				thenReturn(_resultSet);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(DatabaseConnectorConstants.JSON);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME,
                COLUMN_NAME_NAME, COLUMN_NAME_ID, COLUMN_NAME_ID, COLUMN_NAME_NAME, COLUMN_NAME_ID, COLUMN_NAME_NAME,
                COLUMN_NAME_ID);
        Mockito.when(_resultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false).thenReturn(true).thenReturn(
                true).thenReturn(false).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, COLUMN_NAME_NAME)).thenReturn(
                _resultSetColumnNameName);
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(false);

        Mockito.when(_databaseMetaData.getColumns(null, null, OBJECT_TYPE_ID, COLUMN_NAME_ID)).thenReturn(
                _resultSetColumnNameId);
        Mockito.when(_resultSetColumnNameId.next()).thenReturn(true).thenReturn(false);

        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));

        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
    }

    @Test
    public void testExecuteCommitByRowsUpdateOperation() throws SQLException, IOException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testExecuteCommitByProfileUpdateOperation() throws SQLException, IOException {

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_PROFILE);

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();

        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    @Test
    public void testNullAllowDynamicUpdateOperation() {

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": null }], \"WHERE\": [{ \"column\": \"id\", "
                + "\"value\": 191, \"operator\": \"=\" }]}";

        StringBuilder query = new StringBuilder("UPDATE CUSTOMER SET ");

        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        List<ObjectData> batchData = new ArrayList<>();
        for (ObjectData objData : _updateRequest) {
            batchData.add(objData);
        }
        dynamicUpdateOperation.appendKeys(batchData, query, simpleOperationResponse);

        Assert.assertTrue(query.toString().equals("UPDATE CUSTOMER SET name=? WHERE id=?"));
    }

    @Test
    public void testNullKeyDynamicUpdateOperation() {

        String input = "{\"SET\": [{ \"column\": null, \"value\": null }], \"WHERE\": [{ \"column\": \"id\", "
                + "\"value\": 191, \"operator\": \"=\" }]}";

        StringBuilder query = new StringBuilder("UPDATE CUSTOMER SET ");

        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        List<ObjectData> batchData = new ArrayList<>();
        for (ObjectData objData : _updateRequest) {
            batchData.add(objData);
        }

        dynamicUpdateOperation.appendKeys(batchData, query, simpleOperationResponse);


        Assert.assertTrue(query.toString().equals("UPDATE CUSTOMER SET  WHERE id=?"));
    }

    @Test
    public void testIncorrectJsonDynamicUpdateOperation() {

        String input = "{\"SET\": [{ \"column\": null, null: null }], \"WHERE\": [{ \"column\": \"id\", "
                + "\"value\": 191, \"operator\": \"=\" }]}";

        StringBuilder query = new StringBuilder("UPDATE CUSTOMER SET ");

        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        List<ObjectData> batchData = new ArrayList<>();
        for (ObjectData objData : _updateRequest) {
            batchData.add(objData);
        }

        dynamicUpdateOperation.appendKeys(batchData, query, simpleOperationResponse);


        Assert.assertTrue(query.toString().equals("UPDATE CUSTOMER SET "));
    }

    @Test
    public void testDynamicUpdateOperation() {

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": null }, { \"column\": \"address\", \"value\": null }], \"WHERE\": [{ \"column\": \"id\", "
                + "\"value\": 191, \"operator\": \"=\" }]}";

        StringBuilder query = new StringBuilder("UPDATE CUSTOMER SET ");

        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);

        List<ObjectData> batchData = new ArrayList<>();
        for (ObjectData objData : _updateRequest) {
            batchData.add(objData);
        }

        dynamicUpdateOperation.appendKeys(batchData, query, simpleOperationResponse);

        Assert.assertTrue(query.toString().equals("UPDATE CUSTOMER SET name=?,address=? WHERE id=?"));
    }

    /**
     *
     * Test Unique constraint exception
     * @throws IOException
     * @throws SQLException
     */
    @Test
    public void testExecuteCommitByRowsConstraintException() throws IOException,SQLException {
        BatchUpdateException bac = new BatchUpdateException(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE, new int[]{1,2});
        Mockito.when(_preparedStatement.executeBatch()).thenThrow(bac);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);
        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);
        Assert.assertEquals(DataTypesUtil.CONSTRAINT_EXCEPTION_MESSAGE , simpleOperationResponse.getResults().get(0).getMessage());
    }

    /**
     * Test to check if result and payload is success for commit by rows with single document
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testSuccessDynamicUpdatePayloadCommitByRows() throws SQLException, IOException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);
        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE ,
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE ,
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8),
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}");
    }

    /**
     * Test to check if result and payload is success for commit by profile with single document
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testSuccessDynamicUpdatePayloadCommitByProfile() throws SQLException, IOException {
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_PROFILE);
        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_CODE ,
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,DatabaseConnectorConstants.SUCCESS_RESPONSE_MESSAGE ,
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8),
                "{\"Query\":\"UPDATE CUSTOMER SET name=? WHERE id=?\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}");
    }

    /**
     * Test to check if result and payload is success for commit by rows with multiple documents
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadSuccessByRowMultiDocs() throws SQLException, IOException {
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameId.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": \"XYZ\" }], " +
                "\"WHERE\": [{ \"column\": \"id\", \"value\": 191, \"operator\": \"=\" }]}\n";
        List<InputStream> inputs = new ArrayList<>(2);
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        inputs.add(result);
        inputs.add(result);

        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        int playloadIndex = 1;
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH,"Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    "{\"Status \":\"Batch executed successfully\",\"Batch Number \":"+playloadIndex+",\"No of records in batch \":1}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if result and payload is success for commit by profile with multiple documents
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadSuccessByProfileMultiDocs() throws SQLException, IOException {
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameId.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": \"XYZ\" }], " +
                "\"WHERE\": [{ \"column\": \"id\", \"value\": 191, \"operator\": \"=\" }]}\n";
        List<InputStream> inputs = new ArrayList<>(2);
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        inputs.add(result);
        inputs.add(result);

        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_PROFILE);

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,
                    "{\"Query\":\"UPDATE CUSTOMER SET name=? WHERE id=?\",\"Rows Effected\":0,\"Status\":\"Executed Successfully\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
        }
    }

    /**
     * Test to check if result and payload is Failure for commit by rows with single document
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadsFailureCommitByRow() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);
        Mockito.doThrow(new SQLException("java.sql.SQLException")).when(_connection).commit();

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "0",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"{\"errorCode\":0,\"errorMessage\":\"java.sql.SQLException\"}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8) );
    }

    /**
     * Test to check if result and payload is Failure for commit by profile with single document
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadsFailureCommitByProfile() throws SQLException, IOException {

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_PROFILE);
        Mockito.doThrow(new SQLException("java.sql.SQLException")).when(_preparedStatement).executeUpdate();

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "0",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"{\"errorCode\":0,\"errorMessage\":\"java.sql.SQLException\"}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8) );
    }

    /**
     * Test to check if result and payload is Failure for commit by rows with multiple documents
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadsFailureCommitByRowMultiDocs() throws SQLException, IOException {
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameId.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": \"XYZ\" }], " +
                "\"WHERE\": [{ \"column\": \"id\", \"value\": 191, \"operator\": \"=\" }]}\n";
        List<InputStream> inputs = new ArrayList<>(2);
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        inputs.add(result);
        inputs.add(result);

        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_ROWS);
        Mockito.doThrow(new SQLException("java.sql.SQLException")).when(_connection).commit();

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {

            Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "0",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"{\"errorCode\":0,\"errorMessage\":\"java.sql.SQLException\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8) );
        }
    }

    /**
     * Test to check if result and payload is Failure for commit by profile with multiple documents
     *
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void testExecuteDynamicUpdatePayloadsFailureCommitByProfileMultiDocs() throws SQLException, IOException {
        Mockito.when(_resultSetColumnNameName.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetColumnNameId.next()).thenReturn(true).thenReturn(true).thenReturn(false);

        String input = "{\"SET\": [{ \"column\": \"name\", \"value\": \"XYZ\" }], " +
                "\"WHERE\": [{ \"column\": \"id\", \"value\": 191, \"operator\": \"=\" }]}\n";
        List<InputStream> inputs = new ArrayList<>(2);
        InputStream result = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
        inputs.add(result);
        inputs.add(result);

        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);

        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(COMMIT_BY_PROFILE);

        Mockito.doThrow(new SQLException("java.sql.SQLException")).when(_preparedStatement).executeUpdate();

        dynamicUpdateOperation.executeUpdateOperation(_updateRequest, simpleOperationResponse ,_connection);

        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.APPLICATION_ERROR_MESSAGE, OperationStatus.APPLICATION_ERROR,
                    simpleOperationResult.getStatus());
            Assert.assertEquals(DatabaseConnectorTestConstants.STATUS_CODE_MISMATCH, "0",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals(DatabaseConnectorTestConstants.OPERATION_MESSAGE_MISMATCH, "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue(DatabaseConnectorTestConstants.PAYLOAD_METADATA_ASSERT_FAILED,
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals(DatabaseConnectorTestConstants.PAYLOAD_ASSERT_FAILED,"{\"errorCode\":0,\"errorMessage\":\"java.sql.SQLException\"}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8) );
        }
    }

}
