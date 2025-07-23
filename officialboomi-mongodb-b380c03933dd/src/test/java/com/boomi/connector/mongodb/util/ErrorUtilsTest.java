// Copyright (c) 2023 Boomi, LP

package com.boomi.connector.mongodb.util;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.mongodb.TrackedDataWrapper;
import com.boomi.connector.mongodb.bean.ErrorDetails;
import com.mongodb.MongoException;

import org.bson.Document;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class ErrorUtilsTest {

    private static final String ERROR_CODE_INFO = "Error code is different";
    private static final String ERROR_MESSAGE = "Connection failed";
    private static final Integer ERROR_CODE = 102;
    private static final String EXPECTED_ERROR_CODE = "500";
    private ErrorDetails errorDetails;

    @Test
    public void testupdateErrorDetailsinBatch() {
        Integer errorCodeExpected = -3;
        String expectedErrorMessage = ERROR_MESSAGE;

        TrackedData trackedData = mock(TrackedData.class);
        Document document = mock(Document.class);

        TrackedDataWrapper trackedDataWrapper = new TrackedDataWrapper(trackedData, document, ERROR_CODE,
                expectedErrorMessage);
        MongoException mongoConnectionException = new MongoException(expectedErrorMessage);
        List<TrackedDataWrapper> trackedDataWrapperList = Collections.singletonList(trackedDataWrapper);

        ErrorUtils.updateErrorDetailsinBatch(mongoConnectionException, trackedDataWrapperList);

        String actualErrorMessage = trackedDataWrapper.getErrorDetails().getErrorMessage();
        Integer errorCodeActual = trackedDataWrapper.getErrorDetails().getErrorCode();

        assertEquals("Error message is different", expectedErrorMessage, actualErrorMessage);
        assertEquals(ERROR_CODE_INFO, errorCodeExpected, errorCodeActual);
    }

    /**
     * errorDetails is null, expecting constant failure status (500) to return
     */
    @Test
    public void testFetchErrorCodeWhenErrorDetailsIsNull() {
        errorDetails = null;

        String actualErrorCode = ErrorUtils.fetchErrorCode(errorDetails);

        assertEquals(ERROR_CODE_INFO, EXPECTED_ERROR_CODE, actualErrorCode);
    }

    /**
     * errorDetails is not null, passing error code as null expecting constant failure status (500) to return
     */
    @Test
    public void testFetchErrorCodeWhenErrorDetailsNotNullWithNullValue() {
        errorDetails = new ErrorDetails(null, ERROR_MESSAGE);

        String actualErrorCode = ErrorUtils.fetchErrorCode(errorDetails);

        assertEquals(ERROR_CODE_INFO, EXPECTED_ERROR_CODE, actualErrorCode);
    }

    /**
     * errorDetails is not null, verify passed error code is returned
     */
    @Test
    public void testFetchErrorCodeWhenErrorDetailsNotNullWithValue() {
        errorDetails = new ErrorDetails(ERROR_CODE, ERROR_MESSAGE);

        String actualErrorCode = ErrorUtils.fetchErrorCode(errorDetails);

        assertEquals(ERROR_CODE_INFO, ERROR_CODE.toString(), actualErrorCode);
    }
}