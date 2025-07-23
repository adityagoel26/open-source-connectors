// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.operations;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.MutableDynamicPropertyMap;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.controllers.SnowflakeGetController;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.util.StreamUtil;

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

import static com.boomi.connector.api.OperationStatus.APPLICATION_ERROR;
import static com.boomi.connector.api.OperationStatus.FAILURE;
import static com.boomi.connector.api.OperationStatus.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The Class SnowflakeBulkUnloadTest
 *
 * @author s.vanangudi
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({SnowflakeBulkUnloadOperation.class})
public class SnowflakeBulkUnloadTest extends BaseTestOperation {

    /**
     * The Constant OPERATION_NAME.
     */
    private static final String OPERATION_NAME = "bulkUnload";
    /**
     * The Constant SMALL_TABLE.
     */
    private static final String SMALL_TABLE = "GET_OPERATION_TESTER";
    /**
     * The Constant BIG_TABLE.
     */
    private static final String BIG_TABLE = "EMPLOYEE_BC";
    /**
     * The Constant FILTER_JSON.
     */
    private static final String FILTER_JSON = "{\"ID\":\"198423221\"}";
    /**
     * The Constant EMPTY_JSON.
     */
    private static final String EMPTY_JSON = "{}";
    /**
     * The Constant INCORRECT_COL_NAME_JSON.
     */
    private static final String INCORRECT_COL_NAME_JSON = "{\"incorrect\":\"%s\"}";
    /**
     * The Constant INVALID_INPUT.
     */
    private static final String INVALID_INPUT = "invalid";
    /**
     * The Constant DOC_COUNT.
     */
    private static final int DOC_COUNT = 2;
    /**
     * The Constant STR_COMPRESSION.
     */
    private static final String STR_COMPRESSION = "compression";
    /**
     * The Constant STR_FILE_FORMAT_TYPE.
     */
    private static final String STR_FILE_FORMAT_TYPE = "fileFormatType";
    /**
     * The Input Stream List.
     */
    private List<InputStream> inputs;
    /**
     * The SnowflakeBulkUnloadOperation object.
     */
    SnowflakeBulkUnloadOperation _op;
    private static final String AWS_BUCKET_NAME = "awsBucketName";
    private static final String AWS_REGION = "awsRegion";
    private static final String STAGE_NAME = "stageName";

    /**
     * setup the Connection Object
     */
    @Before
    public void setup() {
        testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
        _op = mock(SnowflakeBulkUnloadOperation.class);
    }

    /**
     * Testing the Bulk Unload for External Stage JSON
     */
    @Test
    public void shouldReturnSuccessWhenExternalStagingJSON() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(BIG_TABLE);
        testContext.addOperationProperty(STR_COMPRESSION, "NONE");
        testContext.addOperationProperty(STR_FILE_FORMAT_TYPE, "JSON");
        setInputs(new ByteArrayInputStream(FILTER_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, SUCCESS, 1);
        assertOperation(SUCCESS, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Bulk Unload for External Stage CSV
     */
    @Test
    public void shouldReturnSuccessWhenExternalStagingCSV() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        testContext.addOperationProperty(STR_COMPRESSION, "NONE");
        testContext.addOperationProperty(STR_FILE_FORMAT_TYPE, "CSV");
        setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, SUCCESS, 1);
        response.getResults()
                .forEach(result -> result.setPayloads(Collections.singletonList(setupCsvOutputBytes().getBytes())));
        assertOperationCSVOutputForSmallTable();
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Bulk Unload for External Stage and Unzipping
     */
    @Test
    public void shouldReturnSuccessWhenExternalStagingAndUnzipping() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(BIG_TABLE);
        testContext.addOperationProperty(STR_FILE_FORMAT_TYPE, "JSON");
        testContext.addOperationProperty(STR_COMPRESSION, "GZIP");
        setInputs(new ByteArrayInputStream(FILTER_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, SUCCESS, 1);
        assertOperation(SUCCESS, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Bulk Unload for Internal Stage and Unzipping
     */
    @Test
    public void shouldReturnSuccessWhenInternalStagingAndUnzipping() {
        testContext.removeS3Cred();
        testContext.setInternalStageName();
        testContext.setObjectTypeId(BIG_TABLE);
        testContext.addOperationProperty(STR_FILE_FORMAT_TYPE, "JSON");
        testContext.addOperationProperty(STR_COMPRESSION, "GZIP");
        setInputs(new ByteArrayInputStream(FILTER_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, SUCCESS, 1);
        assertOperation(SUCCESS, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Invalid Input
     */
    @Test
    public void shouldAddFailureWhenInvalidInputEntered() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        setInputs(new ByteArrayInputStream(INVALID_INPUT.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, FAILURE, 0);
        assertOperation(FAILURE, 0, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Invalid Column Name
     */
    @Test
    public void shouldAddApplicationErrorWhenInvalidColNameEntered() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        setInputs(new ByteArrayInputStream(INCORRECT_COL_NAME_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, APPLICATION_ERROR, 1);
        assertOperation(APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Invalid Bucket Name for External Stage
     */
    @Test
    public void shouldAddApplicationErrorWhenInvalidBucketNameEntered() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        testContext.setInvalidBucketName();
        setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, APPLICATION_ERROR, 1);
        assertOperation(APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Invalid Bucket Region for External Stage
     */
    @Test
    public void shouldAddApplicationErrorWhenInvalidBucketRegionEntered() {
        testContext.addS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        testContext.setInvalidBucketRegion();
        setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, APPLICATION_ERROR, 1);
        assertOperation(APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Testing the Invalid Internal Stage Name
     */
    @Test
    public void shouldAddApplicationErrorWhenInvalidInternalStageNameEntered() {
        testContext.removeS3Cred();
        testContext.setObjectTypeId(SMALL_TABLE);
        testContext.setInvalidInternalStage();
        setInputs(new ByteArrayInputStream(EMPTY_JSON.getBytes()));
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        SnowflakeBulkUnloadOperation op = runBulkUnloadOperation(inputs, response, APPLICATION_ERROR, 1);
        assertOperation(APPLICATION_ERROR, 1, DOC_COUNT);
        assertConnectionIsClosed(op.getConnection());
    }

    /**
     * Executes the Bulk Unload Operation
     *
     * @param inputs       the List of input stream
     * @param response     Operation response
     * @param status       the operation status
     * @param payloadCount the payload count
     * @return SnowflakeBulkUnloadOperation object
     */
    private SnowflakeBulkUnloadOperation runBulkUnloadOperation(List<InputStream> inputs,
            ModifiedSimpleOperationResponse response, OperationStatus status, int payloadCount) {
        ModifiedUpdateRequest request = new ModifiedUpdateRequest(inputs, response);
        Mockito.doAnswer((Answer<Void>) inv -> mockResponse(DOC_COUNT, payloadCount, status, inv))
               .when(_op)
               .execute(request, response);
        when(_op.getConnection()).thenReturn(new SnowflakeConnection(testContext));
        _op.execute(request, response);
        results = response.getResults();
        return _op;
    }

    /**
     * Sets the Input Value
     *
     * @param value the input stream
     */
    private void setInputs(InputStream value) {
        inputs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            inputs.add(value);
        }
    }

    /**
     * Tests if exception is thrown when no region passed and valid bucket name set
     * Bucket Name - "S3_TEST_BUCKET", AWS Region - "" (blank)
     */
    @Test
    public void singleDocOverridingDOP() {
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkUnloadOperation snowflakeBulkUnloadOperation = new SnowflakeBulkUnloadOperation(conn);
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(AWS_BUCKET_NAME,"S3_TEST_BUCKET");
        dynamicOpProps.addProperty(AWS_REGION,"");
        SimpleTrackedData inputDoc = new SimpleTrackedData(1,inputs.get(0),null,null,dynamicOpProps);
        SimpleUpdateRequest request = new SimpleUpdateRequest(Collections.singletonList(inputDoc));
        SimpleOperationResponse response = new SimpleOperationResponse();
        response.addTrackedData(inputDoc);

        snowflakeBulkUnloadOperation.executeSizeLimitedUpdate(request,response);
        assertEquals(APPLICATION_ERROR, response.getResults().get(0).getStatus());
        assertEquals("Select a valid AWS Region", response.getResults().get(0).getMessage());
    }

    /**
     * All documents Operation Property for stageName set as "INTERNAL_STAGE"
     * DOP - Dynamic Operation Property
     * 1st document - DOP set as "TEST_STAGE" - validating overriding behaviour
     * 2nd document - DOP set as "" (blank) - throws exception with error message "Enter valid Bucket Name/Stage Name"
     * 3rd document - DOP not set - Operation property value takes precedence - validating not overriding behaviour
     * 4th document - DOP set as "TEST_STAGE" - validating overriding behaviour
     * @throws Exception throws any exception occurred
     */
    @Test
    public void multipleDocumentsWithDynamicOperationProperties() throws Exception {
        inputs = new ArrayList<>();
        SnowflakeConnection conn = new SnowflakeConnection(testContext);
        testContext.setInternalStageName();
        inputs.add(StreamUtil.EMPTY_STREAM);
        inputs.add(StreamUtil.EMPTY_STREAM);
        inputs.add(StreamUtil.EMPTY_STREAM);
        inputs.add(StreamUtil.EMPTY_STREAM);
        SnowflakeBulkUnloadOperation snowflakeBulkUnloadOperation = new SnowflakeBulkUnloadOperation(conn);

        SimpleOperationResponse response = new SimpleOperationResponse();
        SimpleUpdateRequest request = getSimpleUpdateRequestForMultipleDocs(response);

        SnowflakeGetController mockSnowflakeGetController = mock(SnowflakeGetController.class);
        PowerMockito.whenNew(SnowflakeGetController.class).withAnyArguments().thenReturn(mockSnowflakeGetController);

        snowflakeBulkUnloadOperation.executeSizeLimitedUpdate(request,response);

        assertEquals(SUCCESS, response.getResults().get(0).getStatus());
        assertEquals(APPLICATION_ERROR, response.getResults().get(1).getStatus());
        assertEquals("Bucket Name and Stage Name empty. Enter a valid Bucket Name/Stage Name!", response.getResults().get(1).getMessage());
        assertEquals(SUCCESS, response.getResults().get(2).getStatus());
        assertEquals(SUCCESS, response.getResults().get(3).getStatus());
    }

    private SimpleUpdateRequest getSimpleUpdateRequestForMultipleDocs(SimpleOperationResponse response) {
        MutableDynamicPropertyMap dynamicOpProps = new MutableDynamicPropertyMap();
        dynamicOpProps.addProperty(STAGE_NAME,"TEST_STAGE");
        dynamicOpProps.addProperty(AWS_BUCKET_NAME,"");
        SimpleTrackedData inputDoc1 = new SimpleTrackedData(1,inputs.get(0),null,null,dynamicOpProps);

        MutableDynamicPropertyMap dynamicOpProps2 = new MutableDynamicPropertyMap();
        dynamicOpProps2.addProperty(STAGE_NAME,"");
        dynamicOpProps2.addProperty(AWS_BUCKET_NAME,"");
        SimpleTrackedData inputDoc2 = new SimpleTrackedData(2,inputs.get(1),null,null,dynamicOpProps2);

        MutableDynamicPropertyMap dynamicOpProps3 = mock(MutableDynamicPropertyMap.class);
        SimpleTrackedData inputDoc3 = new SimpleTrackedData(3,inputs.get(2),null,null,dynamicOpProps3);
        when(inputDoc3.getDynamicOperationProperties().getProperty(STAGE_NAME)).thenReturn(testContext.getOperationProperties().getProperty(STAGE_NAME));
        when(inputDoc3.getDynamicOperationProperties().getProperty(AWS_BUCKET_NAME)).thenReturn("");

        MutableDynamicPropertyMap dynamicOpProps4 = new MutableDynamicPropertyMap();
        dynamicOpProps4.addProperty(STAGE_NAME,"TEST_STAGE");
        dynamicOpProps4.addProperty(AWS_BUCKET_NAME,"");
        SimpleTrackedData inputDoc4 = new SimpleTrackedData(4,inputs.get(3),null,null,dynamicOpProps4);

        response.addTrackedData(inputDoc1);
        response.addTrackedData(inputDoc2);
        response.addTrackedData(inputDoc3);
        response.addTrackedData(inputDoc4);

        return new SimpleUpdateRequest(Arrays.asList(inputDoc1,inputDoc2,inputDoc3,inputDoc4));
    }
}