// Copyright (c) 2024 Boomi, LP.
package com.boomi.salesforce.rest.operation.composite;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.testutil.OperationResponseFactory;
import com.boomi.connector.testutil.SimpleDeleteRequest;
import com.boomi.connector.testutil.SimpleOperationResponse;
import com.boomi.connector.testutil.SimpleOperationResult;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.boomi.connector.testutil.SimpleUpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.operation.composite.batch.SFCompositeUpdateOperations;
import com.boomi.util.CollectionUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.TestUtil;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SFCompositeCollectionOperationTest {

    private static final String SOBJECT_NAME = "pricebook2";
    private static final String EXTERNAL_ID_FIELD = "Id";
    private static final byte[] INPUT_DOCUMENT_1 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook2\"><Name>name1</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] INPUT_DOCUMENT_2 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook2\"><Name>name2</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] INPUT_DOCUMENT_3 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook2\"><Name>name3</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);

    // UPSERT
    private static final byte[] UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_1 =
            "<result><created>true</created><id>01sHp00000BaLNFIA3</id><success>true</success></result>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_2 =
            "<result><created>true</created><id>01sHp00000BaLNGIA3</id><success>true</success></result>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_3 =
            "<result><created>true</created><id>01sHp00000BaLNKIA3</id><success>true</success></result>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] UPSERT_SUCCESS_RESPONSE_1 =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><result><created>true</created><id>01sHp00000BaLNFIA3"
             + "</id><success>true</success></result><result><created>true</created><id>01sHp00000BaLNGIA3</id"
             + "><success>true</success></result></results>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] UPSERT_SUCCESS_RESPONSE_2 =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><result><created>true</created><id>01sHp00000BaLNKIA3"
             + "</id><success>true</success></result></results>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] UPSERT_ERROR_RESPONSE_PAYLOAD =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Result><success>false</success><sf__Error>[404] [Errors "
             + "occurred while executing Composite upsert request] Field name provided, null does not match an "
             + "External ID for Pricebook2 Error code: 404</sf__Error></Result>").getBytes(StringUtil.UTF8_CHARSET);
    private static final String UPSERT_ERROR_MESSAGE =
            "[Errors occurred while executing Composite upsert request] Field name provided, null does not match an "
            + "External ID for Pricebook2 Error code: 404";
    private static final String UPSERT_ERROR_CODE = "404";

    // DELETE
    private static final byte[] DELETE_ERROR_RESPONSE_1 =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><result><errors><message>entity is "
             + "deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors><id>01sHp00000BaMn0IAF</id"
             + "><success>false</success></result><result><errors><message>entity is "
             + "deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors><id>01sHp00000BaMn1IAF</id"
             + "><success>false</success></result></results>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] DELETE_ERROR_RESPONSE_2 =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><results><result><errors><message>entity is "
             + "deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors><id>01sHp00000BaMn5IAF</id"
             + "><success>false</success></result></results>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] DELETE_EXPECTED_ERROR_OUTPUT_1 =
            ("<result><errors><message>entity is deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors"
             + "><id>01sHp00000BaMn0IAF</id><success>false</success></result>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] DELETE_EXPECTED_ERROR_OUTPUT_2 =
            ("<result><errors><message>entity is deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors"
             + "><id>01sHp00000BaMn1IAF</id><success>false</success></result>").getBytes(StringUtil.UTF8_CHARSET);
    private static final byte[] DELETE_EXPECTED_ERROR_OUTPUT_3 =
            ("<result><errors><message>entity is deleted</message><statusCode>ENTITY_IS_DELETED</statusCode></errors"
             + "><id>01sHp00000BaMn5IAF</id><success>false</success></result>").getBytes(StringUtil.UTF8_CHARSET);

    // CREATE
    private static final byte[] CREATE_ERROR_REQUEST_1 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook22\"><Name>name1</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] CREATE_ERROR_REQUEST_2 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook22\"><Name>name2</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] CREATE_ERROR_REQUEST_3 =
            "<?xml version='1.0' encoding='UTF-8'?><records type=\"pricebook22\"><Name>name3</Name></records>".getBytes(
                    StringUtil.UTF8_CHARSET);
    private static final byte[] CREATE_EXPECTED_ERROR_OUTPUT =
            ("<?xml version=\"1.0\" encoding=\"UTF-8\"?><Result><success>false</success><sf__Error>[500] [Errors "
             + "occurred while executing Composite create request] [500] An unexpected error occurred. Please include"
             + " this ErrorId if you contact support: 1320140941-26724 (-1168987540) Error code: "
             + "500</sf__Error></Result>").getBytes(StringUtil.UTF8_CHARSET);
    private static final String CREATE_ERROR_CODE = "500";
    private static final String CREATE_ERROR_MESSAGE =
            "[Errors occurred while executing Composite create request] [500] An unexpected error occurred. Please "
            + "include this ErrorId if you contact support: 1320140941-26724 (-1168987540) Error code: 500";

    // UPDATE
    private static final String UPDATE_ERROR_REQUEST_1 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><results xmlns:xsi=\"http://www.w3"
             + ".org/2001/XMLSchema-instance\"><result><errors><message>Id not specified in an update "
             + "call</message><statusCode>MISSING_ARGUMENT</statusCode></errors><id "
             + "xsi:nil=\"true\"/><success>false</success></result><result><errors><message>Id not specified in an "
             + "update call</message><statusCode>MISSING_ARGUMENT</statusCode></errors><id "
             + "xsi:nil=\"true\"/><success>false</success></result></results>";
    private static final String UPDATE_ERROR_REQUEST_2 =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><results xmlns:xsi=\"http://www.w3"
             + ".org/2001/XMLSchema-instance\"><result><errors><message>Id not specified in an update "
             + "call</message><statusCode>MISSING_ARGUMENT</statusCode></errors><id "
             + "xsi:nil=\"true\"/><success>false</success></result></results>";
    private static final String UPDATE_EXPECTED_ERROR_OUTPUT =
            "<result><errors><message>Id not specified in an update "
             + "call</message><statusCode>MISSING_ARGUMENT</statusCode></errors><id /><success>false</success"
             + "></result>";

    private static SimpleUpdateRequest simpleUpdateRequest = null;
    private SFRestConnection _connectionMock;
    private AtomConfig _configMock;

    /**
     * Sets up the test environment before running any test methods in this class.
     */
    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    /**
     * Sets up the test environment before each test method.
     */
    @BeforeEach
    void setup() {
        _configMock = mock(AtomConfig.class, Mockito.RETURNS_DEEP_STUBS);
        _connectionMock = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);
        // mock "page size": by setting the value to 2, two request are executed
        when(_configMock.getMaxPageSize()).thenReturn(2);
        when(_connectionMock.getOperationProperties().getBatchCount()).thenReturn(2L);
    }

    /**
     * Test case to verify that the upsert operation returns success.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void upsertShouldReturnSuccess() throws IOException {
        // define three input documents
        SimpleTrackedData data1 = new SimpleTrackedData(0, toStream(INPUT_DOCUMENT_1));
        SimpleTrackedData data2 = new SimpleTrackedData(1, toStream(INPUT_DOCUMENT_2));
        SimpleTrackedData data3 = new SimpleTrackedData(2, toStream(INPUT_DOCUMENT_3));

        List<ObjectData> simpleTrackedDataList = Arrays.asList(data1, data2, data3);
        simpleUpdateRequest = new SimpleUpdateRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data1, data2, data3);

        // mock salesforce responses: one for a batch of two records, the other for a single record
        when(_connectionMock.getRequestHandler().upsertCompositeCollection(any(), any(), any(), any()).getEntity()
                .getContent()).thenReturn(toStream(UPSERT_SUCCESS_RESPONSE_1), toStream(UPSERT_SUCCESS_RESPONSE_2));
        when(_connectionMock.getRequestHandler().upsertCompositeCollection(any(), any(), any(), any())
                .getCode()).thenReturn(200, 200);

        // execute the SUT
        SFCompositeCollectionUpsert compositeUpsert = new SFCompositeCollectionUpsert(_connectionMock,
                simpleUpdateRequest, response, _configMock, SOBJECT_NAME, EXTERNAL_ID_FIELD);
        compositeUpsert.startCompositeOperation(SOBJECT_NAME);

        List<SimpleOperationResult> results = response.getResults();
        assertEquals(2, results.size(), "2 results expected, matching the page size");
        assertEquals(2, results.get(0).getPayloads().size(), "2 payloads expected in the first result");
        assertEquals(1, results.get(1).getPayloads().size(), "1 payload expected in the second result");

        results.forEach(result -> {
            assertEquals(OperationStatus.SUCCESS, result.getStatus());
            assertEquals("200", result.getStatusCode(),
                    "the status code should match the value returned by the mocked response");
            assertEquals("success", result.getMessage());
        });

        assertArrayEquals(UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_1, results.get(0).getPayloads().get(0));
        assertArrayEquals(UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_2, results.get(0).getPayloads().get(1));
        assertArrayEquals(UPSERT_EXPECTED_SUCCESS_OUTPUT_PAYLOAD_3, results.get(1).getPayloads().get(0));
    }

    /**
     * Test case to verify that the upsert operation returns Errors.
     */
    @Test
    public void upsertShouldReturnApplicationErrorOnErrors() {
        // define three input documents
        SimpleTrackedData data1 = new SimpleTrackedData(0, toStream(INPUT_DOCUMENT_1));
        SimpleTrackedData data2 = new SimpleTrackedData(1, toStream(INPUT_DOCUMENT_2));
        SimpleTrackedData data3 = new SimpleTrackedData(2, toStream(INPUT_DOCUMENT_3));

        SimpleOperationResponse response = OperationResponseFactory.get(data1, data2, data3);

        // mock request handler behavior: throw an exception instead of returning a response
        ConnectorException exception = new ConnectorException(UPSERT_ERROR_CODE, UPSERT_ERROR_MESSAGE);
        when(_connectionMock.getRequestHandler().upsertCompositeCollection(any(), any(), any(), any())).thenThrow(
                exception);

        // execute the SUT
        SFCompositeCollectionUpsert compositeUpsert = new SFCompositeCollectionUpsert(_connectionMock,
                simpleUpdateRequest, response, _configMock, SOBJECT_NAME, EXTERNAL_ID_FIELD);
        compositeUpsert.startCompositeOperation(SOBJECT_NAME);

        List<SimpleOperationResult> results = response.getResults();
        assertEquals(2, results.size(), "2 results expected, matching the page size");
        results.forEach(result -> assertEquals(1, result.getPayloads().size(), "each result should contain 1 payload"));

        results.forEach(result -> {
            assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
            assertEquals(UPSERT_ERROR_CODE, result.getStatusCode(),
                    "the error code should match the one set in the ConnectorException");
            assertEquals(buildErrorMessage(UPSERT_ERROR_CODE, UPSERT_ERROR_MESSAGE), result.getMessage(),
                    "the error message concatenates the error code and exception message");
            assertArrayEquals(UPSERT_ERROR_RESPONSE_PAYLOAD, CollectionUtil.getFirst(result.getPayloads()));
        });
    }

    /**
     * Test case to verify that the delete operation returns Error.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void deleteShouldReturnApplicationErrorOnErrors() throws IOException {
        // define three input documents
        final SimpleTrackedData data1 = new SimpleTrackedData(0, "ID1");
        final SimpleTrackedData data2 = new SimpleTrackedData(1, "ID2");
        final SimpleTrackedData data3 = new SimpleTrackedData(2, "ID3");

        List<ObjectIdData> simpleTrackedDataList = Arrays.asList(data1, data2, data3);
        SimpleDeleteRequest simpleDeleteRequest = new SimpleDeleteRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data1, data2, data3);

        // mock salesforce responses: one for a batch of two records, the other for a single record
        when(_connectionMock.getRequestHandler().deleteCompositeCollection(any(), any()).getEntity()
                .getContent()).thenReturn(new ByteArrayInputStream(DELETE_ERROR_RESPONSE_1),
                new ByteArrayInputStream(DELETE_ERROR_RESPONSE_2));
        when(_connectionMock.getRequestHandler().deleteCompositeCollection(any(), any()).getCode()).thenReturn(200);

        // execute the SUT
        SFCompositeCollectionDelete compositeDelete = new SFCompositeCollectionDelete(_connectionMock,
                simpleDeleteRequest, response, _configMock);
        compositeDelete.startCompositeDelete();

        List<SimpleOperationResult> results = response.getResults();
        assertEquals(2, results.size(), "2 results expected, matching the page size");
        assertEquals(2, results.get(0).getPayloads().size(), "2 payloads expected in the first result");
        assertEquals(1, results.get(1).getPayloads().size(), "1 payload expected in the second result");

        results.forEach(result -> {
            assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
            assertEquals(StringUtil.EMPTY_STRING, result.getStatusCode(),
                    "the service does not return an error code for this case");
            assertEquals("entity is deleted ENTITY_IS_DELETED", result.getMessage(),
                    "the error message from the reponse");
        });

        byte[] payload1 = results.get(0).getPayloads().get(0);
        byte[] payload2 = results.get(0).getPayloads().get(1);
        byte[] payload3 = results.get(1).getPayloads().get(0);

        assertArrayEquals(DELETE_EXPECTED_ERROR_OUTPUT_1, payload1);
        assertArrayEquals(DELETE_EXPECTED_ERROR_OUTPUT_2, payload2);
        assertArrayEquals(DELETE_EXPECTED_ERROR_OUTPUT_3, payload3);
    }

    /**
     * Test case to verify that the create operation returns Error.
     */
    @Test
    public void createShouldReturnApplicationErrorOnErrors() {
        // define three input documents
        SimpleTrackedData data1 = new SimpleTrackedData(0, toStream(CREATE_ERROR_REQUEST_1));
        SimpleTrackedData data2 = new SimpleTrackedData(1, toStream(CREATE_ERROR_REQUEST_2));
        SimpleTrackedData data3 = new SimpleTrackedData(2, toStream(CREATE_ERROR_REQUEST_3));

        List<ObjectData> simpleTrackedDataList = Arrays.asList(data1, data2, data3);
        simpleUpdateRequest = new SimpleUpdateRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data1, data2, data3);

        // mock request handler behavior: throw an exception instead of returning a response
        ConnectorException exception = new ConnectorException(CREATE_ERROR_CODE, CREATE_ERROR_MESSAGE);
        when(_connectionMock.getRequestHandler().createCompositeCollection(any(), any())).thenThrow(exception);

        // execute the SUT
        SFCompositeCollectionCreate compositeCreate = new SFCompositeCollectionCreate(_connectionMock,
                simpleUpdateRequest, response, _configMock);
        compositeCreate.startCompositeOperation(SOBJECT_NAME);

        List<SimpleOperationResult> results = response.getResults();
        assertEquals(2, results.size(), "2 results expected, matching the page size");
        results.forEach(result -> assertEquals(1, result.getPayloads().size()));

        results.forEach(result -> {
            assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
            assertEquals(CREATE_ERROR_CODE, result.getStatusCode(),
                    "the error code should match the one set in the ConnectorException");
            assertEquals(buildErrorMessage(CREATE_ERROR_CODE, CREATE_ERROR_MESSAGE), result.getMessage(),
                    "the error message concatenates the error code and exception message");
        });

        results.forEach(result -> assertArrayEquals(CREATE_EXPECTED_ERROR_OUTPUT, result.getPayloads().get(0)));
    }

    /**
     * Test case to verify that the update operation returns Error.
     *
     * @throws IOException if an I/O error occurs
     */
    @Test
    public void updateShouldReturnApplicationErrorOnErrors() throws IOException {
        // define three input documents
        SimpleTrackedData data1 = new SimpleTrackedData(0, toStream(INPUT_DOCUMENT_1));
        SimpleTrackedData data2 = new SimpleTrackedData(1, toStream(INPUT_DOCUMENT_2));
        SimpleTrackedData data3 = new SimpleTrackedData(2, toStream(INPUT_DOCUMENT_3));

        List<ObjectData> simpleTrackedDataList = Arrays.asList(data1, data2, data3);
        simpleUpdateRequest = new SimpleUpdateRequest(simpleTrackedDataList);

        SimpleOperationResponse response = OperationResponseFactory.get(data1, data2, data3);

        // mock salesforce responses: one for a batch of two records, the other for a single record
        when(_connectionMock.getRequestHandler().updateCompositeCollection(any(), any()).getEntity()
                .getContent()).thenReturn(toStream(UPDATE_ERROR_REQUEST_1.getBytes(StringUtil.UTF8_CHARSET)),
                toStream(UPDATE_ERROR_REQUEST_2.getBytes(StringUtil.UTF8_CHARSET)));
        when(_connectionMock.getRequestHandler().updateCompositeCollection(any(), any()).getCode()).thenReturn(200,
                200);

        // execute the SUT
        SFCompositeUpdateOperations compositeUpdate = new SFCompositeCollectionUpdate(_connectionMock,
                simpleUpdateRequest, response, _configMock);
        compositeUpdate.startCompositeOperation(SOBJECT_NAME);

        List<SimpleOperationResult> results = response.getResults();
        assertEquals(2, results.size(), "2 results expected, matching the page size");
        assertEquals(2, results.get(0).getPayloads().size(), "2 payloads expected in the first result");
        assertEquals(1, results.get(1).getPayloads().size(), "1 payload expected in the second result");

        results.forEach(result -> {
            assertEquals(OperationStatus.APPLICATION_ERROR, result.getStatus());
            assertEquals(StringUtil.EMPTY_STRING, result.getStatusCode(),
                    "the service does not return an error code for this case");
            assertEquals("Id not specified in an update call MISSING_ARGUMENT", result.getMessage(),
                    "the error message from the reponse");
        });

        byte[] payload1 = results.get(0).getPayloads().get(0);
        byte[] payload2 = results.get(0).getPayloads().get(1);
        byte[] payload3 = results.get(1).getPayloads().get(0);

        assertEquals(UPDATE_EXPECTED_ERROR_OUTPUT, new String(payload1, StringUtil.UTF8_CHARSET));
        assertEquals(UPDATE_EXPECTED_ERROR_OUTPUT, new String(payload2, StringUtil.UTF8_CHARSET));
        assertEquals(UPDATE_EXPECTED_ERROR_OUTPUT, new String(payload3, StringUtil.UTF8_CHARSET));
    }

    /**
     * Builds an error message string with the given error code and message.
     *
     * @param code    the error code
     * @param message the error message
     * @return the formatted error message string
     */
    private static String buildErrorMessage(String code, String message) {
        return String.format("[%s] %s", code, message);
    }

    /**
     * Converts a byte array payload into an InputStream.
     *
     * @param payload the byte array payload to convert
     * @return an InputStream representing the payload
     */
    private static InputStream toStream(byte[] payload) {
        return new ByteArrayInputStream(payload);
    }
}
