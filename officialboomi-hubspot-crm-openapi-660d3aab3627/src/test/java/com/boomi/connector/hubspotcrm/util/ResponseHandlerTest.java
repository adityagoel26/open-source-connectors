// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.hubspotcrm.util;

import com.boomi.connector.util.ResponseUtil;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;

import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.TrackedData;
import com.boomi.util.StringUtil;

import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Unit tests for the ResponseHandler utility class using parameterized tests.
 * Tests various scenarios of HTTP response handling including success and error cases.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ResponseHandlerTest {

    @Mock
    private OperationResponse operationResponse;
    @Mock
    private TrackedData mockData;
    @Mock
    private CloseableHttpResponse httpResponse;
    @Mock
    private StatusLine statusLine;
    @Mock
    private HttpEntity httpEntity;
    @Mock
    private InputStream inputStream;
    @Mock
    private Payload mockPayload;

    @Mock
    private Logger log;

    @BeforeEach
    public void setUp() throws IOException {
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);
        Mockito.when(httpEntity.getContent()).thenReturn(inputStream);

        try (MockedStatic<ResponseUtil> mockedResponseUtil = Mockito.mockStatic(ResponseUtil.class)) {
            mockedResponseUtil.when(() -> ResponseUtil.toPayload(Mockito.any(InputStream.class))).thenReturn(
                    mockPayload);
        }
    }

    /**
     * Provides test data for HTTP response scenarios.
     *
     * @return Stream of arguments containing test scenarios
     */
    private static Stream<Arguments> provideHttpResponseScenarios() {
        return Stream.of(Arguments.of(200, 200, "OK", OperationStatus.SUCCESS, StringUtil.EMPTY_STRING),
                Arguments.of(201, 201, "Created", OperationStatus.SUCCESS, StringUtil.EMPTY_STRING),
                Arguments.of(400, 200, "Bad Request", OperationStatus.APPLICATION_ERROR,
                        StringUtil.toString(OperationStatus.APPLICATION_ERROR)),
                Arguments.of(401, 200, "Unauthorized", OperationStatus.APPLICATION_ERROR,
                        StringUtil.toString(OperationStatus.APPLICATION_ERROR)),
                Arguments.of(500, 200, "Internal Server Error", OperationStatus.APPLICATION_ERROR,
                        StringUtil.toString(OperationStatus.APPLICATION_ERROR)));
    }

    /**
     * Tests HTTP response handling for various status codes and scenarios.
     *
     * @param statusCode              the HTTP status code to test
     * @param expectedStatus          the expected status code for success
     * @param reasonPhrase            the HTTP reason phrase
     * @param expectedOperationStatus the expected operation status
     * @param expectedErrorMessage    the expected error message
     * @throws IOException if there's an error during test execution
     */
    @ParameterizedTest
    @MethodSource("provideHttpResponseScenarios")
    void testHandleResponse(int statusCode, int expectedStatus, String reasonPhrase,
            OperationStatus expectedOperationStatus, String expectedErrorMessage) throws IOException {
        // Setup
        Mockito.when(statusLine.getStatusCode()).thenReturn(statusCode);
        Mockito.when(statusLine.getReasonPhrase()).thenReturn(reasonPhrase);
        Mockito.when(mockData.getLogger()).thenReturn(log);

        // Execute
        ResponseHandler.handleResponse(operationResponse, mockData, httpResponse, expectedStatus, "create");

        // Verify based on status code
             Mockito.verify(operationResponse).addResult(
                    Mockito.eq(mockData),
                    Mockito.eq(expectedOperationStatus),
                    Mockito.eq(StringUtil.toString(statusCode)),
                    Mockito.eq(reasonPhrase),
                    Mockito.any()
            );
    }

    /**
     * Tests error scenarios with different inputs.
     *
     * @param entityContent   the entity content to use
     * @param expectedMessage the expected error message
     */
    @ParameterizedTest
    @CsvSource({
            "null, Response entity is null", "'', Response content stream is null" })
    void testHandleResponse_ErrorScenarios(String entityContent, String expectedMessage) throws IOException {
        // Setup
        if ("null".equals(entityContent)) {
            Mockito.when(httpResponse.getEntity()).thenReturn(null);
        } else {
            Mockito.when(httpEntity.getContent()).thenReturn(null);
        }

        // Execute and verify
        IOException exception = org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
                () -> ResponseHandler.handleResponse(operationResponse, mockData, httpResponse, HttpStatus.SC_OK,
                        "create"));
        org.junit.jupiter.api.Assertions.assertEquals(expectedMessage, exception.getMessage());
    }
}



