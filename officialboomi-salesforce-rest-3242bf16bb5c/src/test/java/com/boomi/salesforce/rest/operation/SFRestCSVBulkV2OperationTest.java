// Copyright (c) 2024 Boomi, LP.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.testutil.OperationResponseFactory;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.bulkv2.csv.SFBulkV2CSVOperations;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SFRestCSVBulkV2OperationTest {

    private static final String CREATE_JOB_ID = "750Hp00001HoutWIAR";
    private static final String CREATE_JOB_RESPONSE =
            "{\"id\":\"750Hp00001HoutWIAR\",\"operation\":\"insert\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:04:49.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:04:49.000+0000\",\"state\":\"Open\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,"
            + "\"contentUrl\":\"services/data/v58.0/jobs/ingest/750Hp00001HoutWIAR/batches\",\"lineEnding\":\"LF\","
            + "\"columnDelimiter\":\"COMMA\"}";

    private static final String CREATE_JOB_STATUS_PENDING_RESPONSE =
            "{\"id\":\"750Hp00001HoutWIAR\",\"operation\":\"insert\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:04:49.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:04:51.000+0000\",\"state\":\"InProgress\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,\"jobType\":\"V2Ingest\","
            + "\"lineEnding\":\"LF\",\"columnDelimiter\":\"COMMA\",\"numberRecordsProcessed\":10,"
            + "\"numberRecordsFailed\":0,\"retries\":0,\"totalProcessingTime\":224,\"apiActiveProcessingTime\":112,"
            + "\"apexProcessingTime\":0}";
    private static final String CREATE_JOB_STATUS_COMPLETED_RESPONSE =
            "{\"id\":\"750Hp00001HoutWIAR\",\"operation\":\"insert\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:04:49.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:04:52.000+0000\",\"state\":\"JobComplete\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,\"jobType\":\"V2Ingest\","
            + "\"lineEnding\":\"LF\",\"columnDelimiter\":\"COMMA\",\"numberRecordsProcessed\":10,"
            + "\"numberRecordsFailed\":0,\"retries\":0,\"totalProcessingTime\":224,\"apiActiveProcessingTime\":112,"
            + "\"apexProcessingTime\":0}";

    private static final String CREATE_RESULTS_RESPONSE =
            "\"sf__Id\",\"sf__Created\",Name,IsActive\n\"01sHp00000ARQpWIAX\",\"true\",\"New Pricebook 1\","
            + "\"true\"\n\"01sHp00000ARQpXIAX\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpYIAX\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpZIAX\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpaIAH\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpbIAH\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpcIAH\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpdIAH\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpeIAH\",\"true\",\"New Pricebook 1\",\"true\"\n"
            + "\"01sHp00000ARQpfIAH\",\"true\",\"New Pricebook 1\",\"true\"\n";

    private static final String QUERY_JOB_ID = "750Hp00001Hov5XIAR";
    private static final String QUERY_JOB_RESPONSE =
            "{\"id\":\"750Hp00001Hov5XIAR\",\"operation\":\"query\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:54:10.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:54:10.000+0000\",\"state\":\"UploadComplete\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,\"lineEnding\":\"LF\","
            + "\"columnDelimiter\":\"COMMA\"}";

    private static final String QUERY_JOB_STATUS_PENDING_RESPONSE =
            "{\"id\":\"750Hp00001Hov5XIAR\",\"operation\":\"query\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:54:10.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:54:10.000+0000\",\"state\":\"UploadComplete\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,\"jobType\":\"V2Query\","
            + "\"lineEnding\":\"LF\",\"columnDelimiter\":\"COMMA\",\"retries\":0,\"totalProcessingTime\":0}";

    private static final String QUERY_JOB_STATUS_COMPLETED_RESPONSE =
            "{\"id\":\"750Hp00001Hov5XIAR\",\"operation\":\"query\",\"object\":\"Pricebook2\","
            + "\"createdById\":\"005Hp00000fwVPjIAM\",\"createdDate\":\"2023-12-12T19:54:10.000+0000\","
            + "\"systemModstamp\":\"2023-12-12T19:54:13.000+0000\",\"state\":\"JobComplete\","
            + "\"concurrencyMode\":\"Parallel\",\"contentType\":\"CSV\",\"apiVersion\":58.0,\"jobType\":\"V2Query\","
            + "\"lineEnding\":\"LF\",\"columnDelimiter\":\"COMMA\",\"numberRecordsProcessed\":30,\"retries\":0,"
            + "\"totalProcessingTime\":112}";
    private static final String QUERY_RESULT_RESPONSE =
            "\"Id\"\n\"01sHp00000ARQpWIAX\"\n\"01sHp00000ARQpXIAX\"\n\"01sHp00000ARQpYIAX\"\n"
            + "\"01sHp00000ARQpZIAX\"\n\"01sHp00000ARQpaIAH\"";

    private static final String QUERY_INPUT =
            "SELECT Id FROM Pricebook2 WHERE Name = 'New Pricebook 1*Updated with Update Operation*'";
    private static final ContentType CSV_CONTENT_TYPE = ContentType.create("text/csv");
    private static SimpleUpdateRequest simpleUpdateRequest = null;
    private SFRestConnection _connection;

    /**
     * Sets up the test environment before each test method.
     */
    @BeforeEach
    void setup() {
        _connection = mock(SFRestConnection.class, RETURNS_DEEP_STUBS);
    }

    /**
     * Test case to verify the execution of an insert job.
     */
    @Test
    void executeInsertJob() {
        when(_connection.getOperationProperties().getBulkOperation()).thenReturn("insert");

        ClassicHttpResponse createJobResponse = buildResponse(CREATE_JOB_RESPONSE, APPLICATION_JSON);
        when(_connection.getRequestHandler().createBulkV2Job()).thenReturn(createJobResponse);
        ClassicHttpResponse statusPending = buildResponse(CREATE_JOB_STATUS_PENDING_RESPONSE, APPLICATION_JSON);
        ClassicHttpResponse statusCompleted = buildResponse(CREATE_JOB_STATUS_COMPLETED_RESPONSE, APPLICATION_JSON);
        when(_connection.getRequestHandler().getBulkStatus(CREATE_JOB_ID)).thenReturn(statusPending, statusCompleted);
        ClassicHttpResponse csvResultsResponse = buildResponse(CREATE_RESULTS_RESPONSE, CSV_CONTENT_TYPE);
        when(_connection.getRequestHandler().getBulkSuccessResult(CREATE_JOB_ID)).thenReturn(csvResultsResponse);

        SimpleTrackedData data = new SimpleTrackedData(0, StreamUtil.EMPTY_STREAM);

        List<ObjectData> simpleTrackedDataList = Arrays.asList(data);
        simpleUpdateRequest = new SimpleUpdateRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data);

        SFRestCSVBulkV2Operation sfRestCSVBulkV2Operation = new SFRestCSVBulkV2Operation(_connection);
        sfRestCSVBulkV2Operation.executeUpdate(simpleUpdateRequest, response);

        assertResponse(response, CREATE_RESULTS_RESPONSE);
    }

    /**
     * Test case to verify the execution of a query job.
     */
    @Test
    void executeQuery() {
        when(_connection.getOperationProperties().getBulkOperation()).thenReturn("query");

        ClassicHttpResponse createJobResponse = buildResponse(QUERY_JOB_RESPONSE, APPLICATION_JSON);
        when(_connection.getRequestHandler().createBulkV2QueryJob(QUERY_INPUT)).thenReturn(createJobResponse);
        ClassicHttpResponse statusPending = buildResponse(QUERY_JOB_STATUS_PENDING_RESPONSE, APPLICATION_JSON);
        ClassicHttpResponse statusCompleted = buildResponse(QUERY_JOB_STATUS_COMPLETED_RESPONSE, APPLICATION_JSON);
        when(_connection.getRequestHandler().getBulkQueryStatus(QUERY_JOB_ID)).thenReturn(statusPending,
                statusCompleted);
        ClassicHttpResponse csvResultsResponse = buildResponse(QUERY_RESULT_RESPONSE, CSV_CONTENT_TYPE);
        when(_connection.getRequestHandler().getBulkQueryResult(QUERY_JOB_ID, null, 0L)).thenReturn(csvResultsResponse);

        SimpleTrackedData data = new SimpleTrackedData(0,
                new ByteArrayInputStream(QUERY_INPUT.getBytes(StringUtil.UTF8_CHARSET)));

        List<ObjectData> simpleTrackedDataList = Collections.singletonList(data);
        simpleUpdateRequest = new SimpleUpdateRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data);

        SFBulkV2CSVOperations sfBulkV2CSVOperations = new SFBulkV2CSVOperations(_connection);
        sfBulkV2CSVOperations.executeQuery(simpleUpdateRequest, response);

        assertResponse(response, QUERY_RESULT_RESPONSE);
    }

    /**
     * Asserts that the provided SimpleOperationResponse matches the expected results.
     *
     * @param response            the SimpleOperationResponse to assert
     * @param createResultsResponse the expected payload of the response
     */
    private static void assertResponse(SimpleOperationResponse response, String createResultsResponse) {
        assertEquals(1, response.getResults().size());
        SimpleOperationResult result = CollectionUtil.getFirst(response.getResults());
        assertEquals(OperationStatus.SUCCESS, result.getStatus());
        byte[] payload = CollectionUtil.getFirst(result.getPayloads());
        assertEquals(createResultsResponse, new String(payload, StringUtil.UTF8_CHARSET));
    }

    /**
     * Builds a mock HTTP response with the given payload and content type.
     *
     * @param payload     the response payload
     * @param contentType the content type of the response
     * @return the HTTP response
     */
    private static ClassicHttpResponse buildResponse(String payload, ContentType contentType) {
        return new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                ByteArrayInputStream stream = new ByteArrayInputStream(payload.getBytes());
                return new InputStreamEntity(stream, stream.available(), contentType);
            }
        };
    }
}
