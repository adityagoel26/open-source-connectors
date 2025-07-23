// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeCreateController;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.util.SnowflakeOverrideConstants;
import com.boomi.snowflake.util.TestConfig;
import com.boomi.util.StreamUtil;

import com.boomi.util.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SnowflakeBulkLoadOperation.class})
public class SnowflakeBulkLoadTest extends BaseTestOperation {

    private static final String OPERATION_NAME = "bulkLoad";
    private static final String TABLE_NAME = "CREATE_OPERATION_TESTER";
    private static final String CSV_INPUT = "3 \n 7";
    private static final int DOC_COUNT = 2;
    private static final String STR_COLUMNS = "columns";
    private List<InputStream> _inputs;
    private SnowflakeBulkLoadOperation _op;
    private static final String STAGE_NAME = "stageName";
    private static final String AWS_BUCKET_NAME = "awsBucketName";
    private static final String AWS_REGION = "awsRegion";
    private static final String STAGE_VALUE = "TEST_STAGE";
    private static final String DB_VALUE="TEST_SF_DB";
    private static final String ERROR_MSG="Failed to upload files to Snowflake internal stage";

    @Before
    public void setup() {
        testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
        testContext.setObjectTypeId(TABLE_NAME);
        testContext.addConnectionProperty("enablePooling", false);
        _op = Mockito.mock(SnowflakeBulkLoadOperation.class);
    }

    @Test
    public void shouldReturnSuccessWhenExternalStaging() {
        testContext.addOperationProperty(STR_COLUMNS, "ID");
        testContext.addS3Cred();
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkLoadOperation op = runBulkLoadOperation(_inputs, response,
                OperationStatus.SUCCESS, false);
        assertOperation(OperationStatus.SUCCESS, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    @Test
    public void shouldAddApplicationErrorWhenInvalidInputEntered() {
        testContext.addOperationProperty(STR_COLUMNS, "\"CurrentTime\"");
        testContext.addS3Cred();
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkLoadOperation op = runBulkLoadOperation(_inputs, response,
                OperationStatus.APPLICATION_ERROR, false);
        assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    @Test
    public void shouldAddApplicationErrorWhenInvalidColNameEntered() {
        testContext.addOperationProperty(STR_COLUMNS, "invalid");
        testContext.addS3Cred();
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkLoadOperation op = runBulkLoadOperation(_inputs, response,
                OperationStatus.APPLICATION_ERROR, false);
        assertOperation(OperationStatus.APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenInvalidBucketNameEntered() {
        testContext.addS3Cred();
        testContext.setInvalidBucketName();
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        runBulkLoadOperation(_inputs, response, OperationStatus.FAILURE, true);
    }

    @Test(expected = ConnectorException.class)
    public void shouldThrowExceptionWhenInvalidBucketRegionEntered() {
        testContext.addS3Cred();
        testContext.setInvalidBucketRegion();
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        setInputs(new ByteArrayInputStream(CSV_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        runBulkLoadOperation(_inputs, response, OperationStatus.FAILURE, true);
    }

    private SnowflakeBulkLoadOperation runBulkLoadOperation(List<InputStream> inputs,
                                                            ModifiedSimpleOperationResponse response, OperationStatus status, boolean throwException) {
        ModifiedUpdateRequest request = new ModifiedUpdateRequest(inputs, response);
        if (throwException) {
            Mockito.doThrow(ConnectorException.class).when(_op).execute(request, response);
        } else {
            Mockito.doAnswer((Answer<Void>) inv -> mockResponse(DOC_COUNT, 1, status, inv))
                    .when(_op)
                    .execute(request, response);
        }
        Mockito.when(_op.getConnection()).thenReturn(new SnowflakeConnection(testContext));
        _op.execute(request, response);
        results = response.getResults();
        return _op;
    }

    private void setInputs(InputStream value) {
        _inputs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            _inputs.add(value);
        }
    }

    /**
     * All documents Operation Property for stageName set as "INTERNAL_STAGE"
     * DOP - Dynamic Operation Property
     * 1st document - DOP set as "TEST_STAGE" - validating overriding behaviour
     * 2nd document - DOP set as "" (blank) - throws exception with error message "Enter valid Bucket Name/Stage Name"
     * 3rd document - DOP not set - Operation property value takes precedence - validating not overriding behaviour
     * 4th document - DOP set as "TEST_STAGE" - validating overriding behaviour
     *
     * @throws Exception throws any exception occurred
     */
    @Test
    public void multipleDocumentsWithDynamicOperationProperties() throws Exception {
        _inputs = new ArrayList<>();
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        testContext.setInternalStageName();
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);

        SimpleOperationResponse response = new SimpleOperationResponse();
        SimpleUpdateRequest request = getSimpleUpdateRequestForMultipleDocs(response);

        SnowflakeCreateController mockSnowflakeCreateController = Mockito.mock(SnowflakeCreateController.class);
        PowerMockito.whenNew(SnowflakeCreateController.class).withAnyArguments().thenReturn(mockSnowflakeCreateController);

        snowflakeBulkLoadOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(0).getStatus());
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(1).getStatus());
        Assert.assertEquals("Bucket Name and Stage Name empty. Enter a valid Bucket Name/Stage Name!", response.getResults().get(1).getMessage());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(2).getStatus());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(3).getStatus());
    }

    /**
     * Tests if exception is thrown when no region passed and valid bucket name set
     * Bucket Name - "S3_TEST_BUCKET", AWS Region - "" (blank)
     */
    @Test
    public void singleDocOverridingDOP() throws Exception {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(AWS_BUCKET_NAME, "S3_TEST_BUCKET");
        dynamicOpProps.addProperty(AWS_REGION, "");
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);
        snowflakeBulkLoadOperation.executeUpdate(request, response);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
        Assert.assertEquals("Select a valid AWS Region", response.getResults().get(0).getMessage());
    }

    private SimpleUpdateRequest getSimpleUpdateRequestForMultipleDocs(SimpleOperationResponse response) {
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(STAGE_NAME, "TEST_STAGE");
        dynamicOpProps.addProperty(AWS_BUCKET_NAME, "");
        SimpleTrackedData inputDoc1 = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);

        MutableDynamicPropertyMap dynamicOpProps2 = new MutableDynamicPropertyMap();
        dynamicOpProps2.addProperty(STAGE_NAME, "");
        dynamicOpProps2.addProperty(AWS_BUCKET_NAME, "");
        SimpleTrackedData inputDoc2 = new SimpleTrackedData(2, _inputs.get(1), null, null, dynamicOpProps2);

        MutableDynamicPropertyMap dynamicOpProps3 = Mockito.mock(MutableDynamicPropertyMap.class);
        SimpleTrackedData inputDoc3 = new SimpleTrackedData(3, _inputs.get(2), null, null, dynamicOpProps3);
        Mockito.when(inputDoc3.getDynamicOperationProperties().getProperty(STAGE_NAME)).thenReturn(testContext.getOperationProperties().getProperty(STAGE_NAME));
        Mockito.when(inputDoc3.getDynamicOperationProperties().getProperty(AWS_BUCKET_NAME)).thenReturn("");

        MutableDynamicPropertyMap dynamicOpProps4 = new MutableDynamicPropertyMap();
        dynamicOpProps4.addProperty(STAGE_NAME, "TEST_STAGE");
        dynamicOpProps4.addProperty(AWS_BUCKET_NAME, "");
        SimpleTrackedData inputDoc4 = new SimpleTrackedData(4, _inputs.get(3), null, null, dynamicOpProps4);

        response.addTrackedData(inputDoc1);
        response.addTrackedData(inputDoc2);
        response.addTrackedData(inputDoc3);
        response.addTrackedData(inputDoc4);

        return new SimpleUpdateRequest(Arrays.asList(inputDoc1, inputDoc2, inputDoc3, inputDoc4));
    }

    /**
     * Creates a SimpleUpdateRequest with multiple SimpleTrackedData objects representing different documents and database configurations.
     * This method generates four SimpleTrackedData objects with varying database values and adds them to the provided SimpleOperationResponse.
     *
     * @param response The SimpleOperationResponse to which the tracked data will be added. This object will be modified by the method.
     * @return A SimpleUpdateRequest containing four SimpleTrackedData objects with different configurations.
     *
     * @implNote This method assumes that the _inputs list contains at least 4 elements.
     *           It uses "DEV" as the database value for three of the documents and "DEF" for one.
     */

    private SimpleUpdateRequest getSimpleUpdateRequestForMultipleDocsAndDbAndSchema(SimpleOperationResponse response) {
        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(DB_VALUE, "DEV");

        SimpleTrackedData inputDoc1 = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);

        MutableDynamicPropertyMap dynamicOpProps2 = createDynamicPropertyMap(DB_VALUE, "DEF");
        SimpleTrackedData inputDoc2 = new SimpleTrackedData(2, _inputs.get(1), null, null, dynamicOpProps2);

        MutableDynamicPropertyMap dynamicOpProps3 = createDynamicPropertyMap(DB_VALUE, "DEV");
        SimpleTrackedData inputDoc3 = new SimpleTrackedData(3, _inputs.get(2), null, null, dynamicOpProps3);


        MutableDynamicPropertyMap dynamicOpProps4 =createDynamicPropertyMap(DB_VALUE, "DEF");
        SimpleTrackedData inputDoc4 = new SimpleTrackedData(4, _inputs.get(3), null, null, dynamicOpProps4);

        response.addTrackedData(inputDoc1);
        response.addTrackedData(inputDoc2);
        response.addTrackedData(inputDoc3);
        response.addTrackedData(inputDoc4);

        return new SimpleUpdateRequest(Arrays.asList(inputDoc1, inputDoc2, inputDoc3, inputDoc4));
    }
    /**
     * Tests Operation status as SUCCESS for multiple documents , db and schema values .
     */
    @Test
    public void multipleDocumentsWithDynamicOperationPropertiesDbAndSchema() throws Exception {
        _inputs = new ArrayList<>();
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        testContext.setInternalStageName();
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        _inputs.add(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);

        SimpleOperationResponse response = new SimpleOperationResponse();
        SimpleUpdateRequest request = getSimpleUpdateRequestForMultipleDocsAndDbAndSchema(response);

        SnowflakeCreateController mockSnowflakeCreateController = Mockito.mock(SnowflakeCreateController.class);
        PowerMockito.whenNew(SnowflakeCreateController.class).withAnyArguments().thenReturn(mockSnowflakeCreateController);

        snowflakeBulkLoadOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(0).getStatus());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(1).getStatus());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(1).getStatus());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(2).getStatus());
        Assert.assertEquals(OperationStatus.SUCCESS, response.getResults().get(3).getStatus());
    }


    /**
     * Tests if exception is thrown when Snowflake fails to run the COPY INTO command
     */
    @Test
    public void singleDocOverridingDOPWithSchemaAndDB() {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);

        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");

        setContext();
        setInputs(StreamUtil.EMPTY_STREAM);

        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);
        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(
                DB_VALUE,
                "DEV");
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);
        snowflakeBulkLoadOperation.executeUpdate(request, response);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
        String responseMessage = response.getResults().get(0).getMessage();
        Assert.assertTrue(responseMessage.contains(ERROR_MSG));
    }

    /**
     * Tests if exception is thrown when schema value in the connection is empty.
     */
    @Test
    public void singleDocOverridingDOPWithEmptySchemaAndDB() {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);

        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");

        setContext();
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);
        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(DB_VALUE, StringUtil.EMPTY_STRING);
        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);
        snowflakeBulkLoadOperation.executeUpdate(request, response);
        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }

    /**
     * Tests if exception is thrown when schema and db values in the connection are empty.
     */
    @Test
    public void singleDocOverridingDOPWithEmptySchemaAndEmptyDB() {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);

        TestConfig.createDbAndSchema("DEV", "EMPLOYEES");
        TestConfig.createDbAndSchema("DEF", "EMPLOYEES");

        setContext();
        setInputs(StreamUtil.EMPTY_STREAM);

        SnowflakeBulkLoadOperation snowflakeBulkLoadOperation = new SnowflakeBulkLoadOperation(conn);
        MutableDynamicPropertyMap dynamicOpProps = createDynamicPropertyMap(StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING);

        SimpleTrackedData inputDoc = new SimpleTrackedData(1, _inputs.get(0), null, null, dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeBulkLoadOperation.executeUpdate(request, response);

        Assert.assertEquals(OperationStatus.APPLICATION_ERROR, response.getResults().get(0).getStatus());
    }

    /**
     * Sets the context to connect to the in memory DB.
     */
    private void setContext() {

        testContext.addConnectionProperty("connectionString", "jdbc:h2:mem:TEST_SF_DB");
        testContext.addConnectionProperty("user", System.getProperty("UserName", "sa"));
        testContext.addConnectionProperty("password", System.getProperty("Password", "pwd"));
        testContext.addConnectionProperty("db", "\"TEST_SF_DB\"");
        testContext.addConnectionProperty("schema", "DEV");
        testContext.addConnectionProperty("enablePooling", false);

    }

    /**
     * Creates a dynamic property map with the given database and schema values.
     *
     * @param db   the database value
     * @param schema the schema value
     * @return the created dynamic property map
     */
    private static MutableDynamicPropertyMap createDynamicPropertyMap(String db, String schema){

        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(STAGE_NAME, STAGE_VALUE);
        dynamicOpProps.addProperty(AWS_BUCKET_NAME, StringUtil.EMPTY_STRING);

        dynamicOpProps.addProperty(SnowflakeOverrideConstants.DATABASE, db);
        dynamicOpProps.addProperty(SnowflakeOverrideConstants.SCHEMA, schema);
        return dynamicOpProps;

    }
}


