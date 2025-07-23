// Copyright (c) 2025 Boomi, LP
package com.boomi.connector.databaseconnector.operations.upsert;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.databaseconnector.connection.DatabaseConnectorConnection;
import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.connector.testutil.DataTypesUtil;
import com.boomi.connector.testutil.ResponseUtil;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * This Class is used to test {@link UpsertOperation Class}
 */
public class UpsertOperationTest {

    private static final String INPUT =
            "{\r\n" + "				\"id\":\"123\",\r\n" + "				\"name\":\"abc" + "\",\r\n"
                    + "				\"dob\":\"2019-12-09\",\r\n" + "				\"clob\":\"Hello\",\r\n"
                    + "				\"isqualified\":true,\r\n" + "				\"laptime\":\"21:09:08\"\r\n"
                    + "		" + "		\r\n" + "            }";

    private static final String OBJECT_TYPE_ID = "CUSTOMER";
    private static final String CATALOG = "CATALOG";
    private static final String COLUMN_NAME_NAME = "name";
    private static final String COLUMN_NAME_ISQUALIFIED = "isqualified";
    private final OperationContext            _operationContext            = Mockito.mock(OperationContext.class);
    private final DatabaseConnectorConnection _databaseConnectorConnection = Mockito.mock(DatabaseConnectorConnection.class);
    private final Connection                  _connection                  = Mockito.mock(Connection.class);
    private final UpdateRequest _updateRequest = Mockito.mock(UpdateRequest.class);
    private final PropertyMap _propertyMap = Mockito.mock(PropertyMap.class);
    private final DatabaseMetaData _databaseMetaData = Mockito.mock(DatabaseMetaData.class);
    private final PreparedStatement _preparedStatement = Mockito.mock(PreparedStatement.class);
    private final ResultSet _resultSet = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetCommonUpsert = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetPrimaryKey = Mockito.mock(ResultSet.class);
    private final ResultSet _resultSetUniqueKey = Mockito.mock(ResultSet.class);
    private final ResultSet resultSetSchemaNull = Mockito.mock(ResultSet.class);
    private final UpsertOperation upsertOperation = new UpsertOperation(_databaseConnectorConnection);
    private final SimpleOperationResponse simpleOperationResponse = new SimpleOperationResponse();

    @Before
    public void setup() throws SQLException {

        Mockito.when(upsertOperation.getContext()).thenReturn(_operationContext);
        Mockito.when(_operationContext.getOperationProperties()).thenReturn(_propertyMap);
        Mockito.when(_operationContext.getObjectTypeId()).thenReturn(OBJECT_TYPE_ID);
        Mockito.when(_propertyMap.getLongProperty(Mockito.anyString())).thenReturn(1L);
        Mockito.when(_databaseConnectorConnection.getDatabaseConnection()).thenReturn(_connection);
        Mockito.when(_connection.getMetaData()).thenReturn(_databaseMetaData);
        Mockito.when(_connection.prepareStatement(Mockito.anyString())).thenReturn(_preparedStatement);
        Mockito.when(_connection.prepareStatement(Mockito.anyString(), Mockito.anyInt())).thenReturn(_preparedStatement);
        Mockito.when(_connection.getCatalog()).thenReturn(CATALOG);
        Mockito.when(_connection.getSchema()).thenReturn(DatabaseConnectorConstants.SCHEMA_NAME);
        Mockito.when(_connection.getMetaData().getPrimaryKeys(CATALOG, null, OBJECT_TYPE_ID)).thenReturn(resultSetSchemaNull);
        Mockito.when(resultSetSchemaNull.next()).thenReturn(true, false);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_preparedStatement.executeBatch()).thenReturn(new int[1]);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.DATA_TYPE)).thenReturn("12");
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.TYPE_NAME)).thenReturn(
                DatabaseConnectorConstants.JSON, DatabaseConnectorConstants.JSON, DatabaseConnectorConstants.NVARCHAR,
                DatabaseConnectorConstants.NVARCHAR);
        Mockito.when(_resultSet.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME,
                COLUMN_NAME_ISQUALIFIED, COLUMN_NAME_NAME, COLUMN_NAME_NAME, COLUMN_NAME_ISQUALIFIED, COLUMN_NAME_ISQUALIFIED);
        Mockito.when(_resultSet.next()).thenReturn(true, true, false, true, true,false);
        InputStream result = new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8));
        DataTypesUtil.setupTrackedData(result, _updateRequest, simpleOperationResponse);
    }

    private void setUpCommonUpsert() throws SQLException {
        setPrimaryKeyResultSet();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MSSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, null, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getIndexInfo(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, true, false)).thenReturn(
                _resultSetUniqueKey);
        Mockito.when(_databaseMetaData.getIndexInfo(CATALOG, null, OBJECT_TYPE_ID, true, false)).thenReturn(
                _resultSetUniqueKey);
        Mockito.when(_resultSetUniqueKey.getString(DatabaseConnectorConstants.NON_UNIQUE)).thenReturn("f");
        Mockito.when(_resultSetUniqueKey.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetUniqueKey.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_ISQUALIFIED);
    }

    private void setUpDataForPostGreUpsert() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        setPrimaryKeyResultSet();
        Mockito.when(_resultSetPrimaryKey.isBeforeFirst()).thenReturn(true).thenReturn(false);
    }

    private void setPrimaryKeyResultSet() throws SQLException {
        Mockito.when(_databaseMetaData.getPrimaryKeys(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID)).thenReturn(_resultSetPrimaryKey);
        Mockito.when(_resultSetPrimaryKey.next()).thenReturn(true).thenReturn(false);
        Mockito.when(_resultSetPrimaryKey.getString(DatabaseConnectorConstants.COLUMN_NAME)).thenReturn(COLUMN_NAME_NAME);
    }

    /**
     * Tests the execution of common upsert operation with commit by rows option.
     * Verifies that the operation completes successfully with the expected status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByRowsCommonUpsert() throws SQLException {
        setUpCommonUpsert();
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID,null))
                .thenReturn(_resultSet);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of common upsert operation with commit by rows option when schema is null.
     * Verifies that the operation successfully processes the request and returns the expected status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByRowsSchemaNullCommonUpsert() throws SQLException {
        setUpCommonUpsert();
        Mockito.when(_connection.getSchema()).thenReturn(null);
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of common upsert operation with commit by profile option.
     * Verifies that the operation successfully processes the request and returns
     * the expected success status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByProfileCommonUpsert() throws SQLException {
        setUpCommonUpsert();
        Mockito.when(_preparedStatement.executeQuery()).thenReturn(_resultSet);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID,null))
                .thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(
                DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of PostgreSQL upsert operation with commit by rows option.
     * Verifies that the operation successfully processes the request and returns
     * the expected success status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByProfilePostgresUpsert() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.POSTGRESQL);
        setUpDataForPostGreUpsert();
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of PostgreSQL upsert operation with commit by rows option.
     * Verifies that the operation successfully processes the request and returns
     * the expected success status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByRowsPostgresUpsert() throws SQLException {
        setUpDataForPostGreUpsert();
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL upsert operation with commit by profile option.
     * Verifies that the operation successfully processes the request and returns
     * the expected success status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByProfileMysqlUpsert() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Tests the execution of MySQL upsert operation with commit by rows option.
     * Verifies that the operation successfully processes the request and returns
     * the expected success status.
     *
     * @throws SQLException if database operation fails
     */
    @Test
    public void testExecuteCommitByRowsMysqlUpsert() throws SQLException {
        setUpDataForPostGreUpsert();
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        OperationStatus operationStatus = simpleOperationResponse.getResults().get(0).getStatus();
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS, operationStatus);
    }

    /**
     * Test to check if payload and result is success for commit by profile
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPayloadSuccessByProfile() throws SQLException, IOException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "Ok",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());

        // Read the expected JSON from test/resources/UpsertResponseForCommitByProfile.json
        String expectedJson = new String(Files.readAllBytes(Paths.get("src/test/resource/UpsertResponseForCommitByProfile.json")), StandardCharsets.UTF_8);
        String normalizedExpectedJson = ResponseUtil.normalizeJson(expectedJson);

        String actualJson = new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8).trim();
        String normalizedActualJson = ResponseUtil.normalizeJson(actualJson);

        Assert.assertEquals("Payload assert failed", normalizedExpectedJson, normalizedActualJson);
    }

    /**
     * Test to check if payload and result is success for commit by row
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPayloadSuccessByRow() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "200",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "Ok",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertEquals("Payload assert failed",
                "{\"Status \":\"Batch executed successfully\",\"Batch Number \":1,\"No of records in batch \":1}",
                new String(simpleOperationResponse.getResults().get(0).getPayloads().get(0), StandardCharsets.UTF_8));
    }

    /**
     * Test to check if payload and result is success for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadSuccessByProfileMultiDocs() throws SQLException, IOException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();

        // Read the expected JSON payload from test/resources/UpsertResponseForCommitByProfileMultiDocs.json
        String expectedJson = new String(Files.readAllBytes(Paths.get("src/test/resource/UpsertResponseForCommitByProfileMultiDocs.json")), StandardCharsets.UTF_8);

        String normalizedExpectedJson = ResponseUtil.normalizeJson(expectedJson);

        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            String actualJson = new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8);
            String normalizedActualJson = ResponseUtil.normalizeJson(actualJson);

            Assert.assertEquals("Payload assert failed", normalizedExpectedJson, normalizedActualJson);
        }
    }

    /**
     * Test to check if payload and result is success for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadSuccessByRowMultiDocs() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        int playloadIndex = 1;
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals(DataTypesUtil.ERROR_MESSAGE, OperationStatus.SUCCESS,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "200",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "Ok",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertEquals("Payload assert failed",
                    "{\"Status \":\"Batch executed successfully\",\"Batch Number \":"+playloadIndex+",\"No of records in batch \":1}",
                    new String(simpleOperationResult.getPayloads().get(0), StandardCharsets.UTF_8));
            playloadIndex = playloadIndex + 1;
        }
    }

    /**
     * Test to check if payload and result is failure for commit by profile
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadFailureByProfile() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue("Payload assert failed",
                simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for commit by row
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadFailureByRow() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                simpleOperationResponse.getResults().get(0).getStatus());
        Assert.assertEquals("Status code assert failed", "",
                simpleOperationResponse.getResults().get(0).getStatusCode());
        Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                simpleOperationResponse.getResults().get(0).getMessage());
        Assert.assertTrue("Payload metadata assert failed",
                simpleOperationResponse.getPayloadMetadatas().isEmpty());
        Assert.assertTrue("Payload assert failed",
                simpleOperationResponse.getResults().get(0).getPayloads().isEmpty());
    }

    /**
     * Test to check if payload and result is failure for commit by profile with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadFailureByProfileMultiDocs() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_PROFILE);
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue("Payload assert failed",
                    simpleOperationResult.getPayloads().isEmpty());
        }
    }

    /**
     * Test to check if payload and result is failure for commit by row with multiple documents
     *
     * @throws SQLException sql exception
     */
    @Test
    public void testExecuteUpsertPlayloadFailureByRowMultiDocs() throws SQLException {
        Mockito.when(_databaseMetaData.getDatabaseProductName()).thenReturn(DatabaseConnectorConstants.MYSQL);
        Mockito.when(_databaseMetaData.getColumns(CATALOG, DatabaseConnectorConstants.SCHEMA_NAME, OBJECT_TYPE_ID, null)).thenReturn(_resultSet);
        Mockito.when(_propertyMap.getProperty(DatabaseConnectorConstants.COMMIT_OPTION)).thenReturn(DatabaseConnectorConstants.COMMIT_BY_ROWS);
        List<InputStream> inputs = new ArrayList<>(2);
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        inputs.add(new ByteArrayInputStream(INPUT.getBytes(StandardCharsets.UTF_8)));
        Mockito.doThrow(SQLException.class).when(_connection).setAutoCommit(false);
        DataTypesUtil.setupMultipleTrackedData(inputs, _updateRequest, simpleOperationResponse);
        upsertOperation.executeSizeLimitedUpdate(_updateRequest, simpleOperationResponse);
        List<SimpleOperationResult> results = simpleOperationResponse.getResults();
        for (SimpleOperationResult simpleOperationResult : results) {
            Assert.assertEquals("Assert failed status", OperationStatus.FAILURE,
                    simpleOperationResult.getStatus());
            Assert.assertEquals("Status code assert failed", "",
                    simpleOperationResult.getStatusCode());
            Assert.assertEquals("Message assert failed", "java.sql.SQLException",
                    simpleOperationResult.getMessage());
            Assert.assertTrue("Payload metadata assert failed",
                    simpleOperationResult.getPayloadMetadatas().isEmpty());
            Assert.assertTrue("Payload assert failed",
                    simpleOperationResult.getPayloads().isEmpty());
        }
    }
}
