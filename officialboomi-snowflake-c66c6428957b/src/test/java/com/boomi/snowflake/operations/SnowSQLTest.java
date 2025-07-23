// Copyright (c) 2022 Boomi, Inc.

package com.boomi.snowflake.operations;

import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.snowflake.SnowflakeConnection;
import com.boomi.snowflake.util.ModifiedSimpleOperationResponse;
import com.boomi.snowflake.util.ModifiedUpdateRequest;
import com.boomi.snowflake.util.SnowflakeContextIT;
import com.boomi.snowflake.wrappers.SnowflakeWrapper;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.boomi.connector.api.OperationStatus.APPLICATION_ERROR;
import static com.boomi.connector.api.OperationStatus.FAILURE;
import static com.boomi.connector.api.OperationStatus.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({SnowflakeSnowSQLOperation.class})
public class SnowSQLTest extends BaseTestOperation {

    private static final String OPERATION_NAME = "snowSQL";
    private static final String SNOW_SQL_SCRIPT_PROP = "sqlScript";
    private static final String SAMPLE_SELECT = "select * from employe;";
    private static final String SAMPLE_INSERT =
            " insert into employe values(1,'anas', '1234', 'password', 533, 'asdasd');";
    private static final String SAMPLE_PARAMETERIZED_SQL_SCRIPT = "$param;$param2;$param;$param3;";
    private static final String SAMPLE_INPUT_STREAM = "{\"$param\":\"%s\",\"$param2\":\"%s\",\"$param3\":\"%s\"}";
    private static final String SAMPLE_SQL_SYNTAX_ERROR = "error";
    private static final int DOC_COUNT = 5;
    private List<InputStream> inputs;
    private static final String SAMPLE_MULTIPLE_SQL_SCRIPT = "INSERT INTO \"SOLUTIONS_DB\".\"PUBLIC\".PRODUCTLOGLIST (PRODUCT_ID,PRODUCT_DESCRIPTION,"
            + "PRODUCT_NAME,STATUS_CHECK) VALUES ($param,$param1,$param2,$param3);"
            + "SELECT * FROM \"SOLUTIONS_DB\".\"PUBLIC\".PRODUCTLOGLIST WHERE \"PRODUCT_NAME\" LIKE $param4;"
            + "DELETE  FROM \"SOLUTIONS_DB\".\"PUBLIC\".\"PRODUCTLOGLIST\" WHERE \"PRODUCT_NAME\" LIKE $param5;";

    @Before
    public void setup() {
        testContext = new SnowflakeContextIT(OperationType.EXECUTE, OPERATION_NAME);
    }

    @Test
    public void MixedStatementsBatchSizeIsOne() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation =
                runSnowSql(SAMPLE_INSERT + SAMPLE_SELECT + SAMPLE_INSERT + SAMPLE_SELECT + SAMPLE_INSERT, 5, SUCCESS);
        assertOperation(SUCCESS, 5, DOC_COUNT);
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void SingleDQLBatchSizeIsOne() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_SELECT, 1, SUCCESS);
        assertOperation(SUCCESS, 1, DOC_COUNT);
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void SingleDMLBatchSizeIsTwo() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_INSERT, 0, SUCCESS);

        assertOperation(SUCCESS, 0, DOC_COUNT);
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void SingleDQLBatchSizeIsTwo() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_INSERT, 0, SUCCESS);
        assertOperation(SUCCESS, 0, DOC_COUNT);
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void MixedStatementsBatchSizeIsTwo() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_SELECT + SAMPLE_INSERT, 0, SUCCESS);
        assertOperation(SUCCESS, 0, 5);
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void ShouldAddApplicationErrorWhenOneOfTheBatchsIsIncorrect() {
        setInputs(new ByteArrayInputStream(
                String.format(SAMPLE_INPUT_STREAM, SAMPLE_SELECT, SAMPLE_INSERT, SAMPLE_SELECT)
                      .getBytes(StandardCharsets.UTF_8)));
        inputs.set(2, new ByteArrayInputStream(
                String.format(SAMPLE_INPUT_STREAM, SAMPLE_SQL_SYNTAX_ERROR, SAMPLE_SQL_SYNTAX_ERROR,
                        SAMPLE_SQL_SYNTAX_ERROR).getBytes(StandardCharsets.UTF_8)));
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_PARAMETERIZED_SQL_SCRIPT, 2, APPLICATION_ERROR);
        assertEquals(APPLICATION_ERROR, results.get(3).getStatus());
        assertConnectionIsClosed(operation.getConnection());
    }

    @Test
    public void MixedStatementsBatchSizeIsSeven() {
        setInputs(StreamUtil.EMPTY_STREAM);
        SnowflakeSnowSQLOperation operation = runSnowSql(SAMPLE_SELECT + SAMPLE_INSERT + SAMPLE_SELECT, 0, SUCCESS);
        assertOperation(SUCCESS, 0, DOC_COUNT);
        assertConnectionIsClosed(operation.getConnection());
    }

    private void setInputs(InputStream value) {
        inputs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            inputs.add(value);
        }
    }

    private SnowflakeSnowSQLOperation runSnowSql(String sqlScript, int payloadCount, OperationStatus status) {
        testContext.addOperationProperty(SNOW_SQL_SCRIPT_PROP, sqlScript);
        SnowflakeSnowSQLOperation op = mock(SnowflakeSnowSQLOperation.class);
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        ModifiedUpdateRequest request = new ModifiedUpdateRequest(inputs, response);
        Mockito.doAnswer((Answer<Void>) inv -> mockResponse(DOC_COUNT, payloadCount, status, inv))
               .when(op)
               .execute(request, response);
        when(op.getConnection()).thenReturn(new SnowflakeConnection(testContext));
        op.execute(request, response);

        results = response.getResults();
        return op;
    }

    private void setInputs(List<InputStream> inputs, String value) {
        inputs.add(new ByteArrayInputStream((value).getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testAllSuccessDocuments() throws Exception {
        inputs = new ArrayList<>();
        String doc1 = "{\"param\":\"5\",\"param1\":\"test_desc\",\"param2\":\"test_name\","
                + "\"param3\":\"0\",\"param4\":\"test_name\",\"param5\":\"test_name\"};";
        String doc2 = "{\"param\":\"6\",\"param1\":\"test_desc1\",\"param2\":\"test_name1\","
                + "\"param3\":\"0\",\"param4\":\"test_name1\",\"param5\":\"test_name1\"};";
        setInputs(inputs,doc1);
        setInputs(inputs,doc2);
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        ModifiedUpdateRequest request = new ModifiedUpdateRequest(inputs, response);
        SnowflakeConnection connection = new SnowflakeConnection(testContext);
        SnowflakeSnowSQLOperation op = new SnowflakeSnowSQLOperation(connection);

        SnowflakeWrapper mockSnowflakeWrapper = mock(SnowflakeWrapper.class);
        PowerMockito.whenNew(SnowflakeWrapper.class).withAnyArguments().thenReturn(mockSnowflakeWrapper);

        testContext.addOperationProperty(SNOW_SQL_SCRIPT_PROP,SAMPLE_MULTIPLE_SQL_SCRIPT);
        testContext.addOperationProperty("numberOfNonZeroScripts",3L);
        testContext.addOperationProperty("numberOfScripts",3L);

        op.executeSizeLimitedUpdate(request, response);
        assertEquals(2, response.getResults().size());
        for (SimpleOperationResult res : response.getResults()) {
            assertEquals(SUCCESS,res.getStatus());
        }
    }

    @Test
    public void testOneSuccessAndTwoFailureDocuments() throws Exception {
        inputs = new ArrayList<>();
        String doc2 = "{\"param\":\"5\",\"param1\":\"test_desc\",\"param2\":\"test_name\","
                + "\"param3\":\"0\",\"param4\":\"test_name\",\"param5\":\"test_name\"};";
        String doc3 = "{\"param\":\"6\",\"param1\":\"test_desc\"};";
        inputs.add(StreamUtil.EMPTY_STREAM);
        setInputs(inputs,doc2);
        setInputs(inputs,doc3);
        ModifiedSimpleOperationResponse response = new ModifiedSimpleOperationResponse();
        ModifiedUpdateRequest request = new ModifiedUpdateRequest(inputs, response);
        SnowflakeConnection connection = new SnowflakeConnection(testContext);
        SnowflakeSnowSQLOperation op = new SnowflakeSnowSQLOperation(connection);

        SnowflakeWrapper mockSnowflakeWrapper = mock(SnowflakeWrapper.class);
        PowerMockito.whenNew(SnowflakeWrapper.class).withAnyArguments().thenReturn(mockSnowflakeWrapper);

        testContext.addOperationProperty(SNOW_SQL_SCRIPT_PROP,SAMPLE_MULTIPLE_SQL_SCRIPT);

        op.executeSizeLimitedUpdate(request, response);
        assertEquals(3, response.getResults().size());
        assertEquals(FAILURE,response.getResults().get(0).getStatus());
        assertEquals(SUCCESS,response.getResults().get(1).getStatus());
        assertEquals(FAILURE,response.getResults().get(2).getStatus());
    }
}