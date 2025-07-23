// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.util.StreamUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;

import static com.boomi.util.StreamUtil.EMPTY_STREAM;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class RequestHandlerTest {

    private static final String JOB_ID = "theJobID";
    private static final String OBJECT_NAME = "theObjectName";
    private static final String ASSIGMENT_RULE_ID = "theAssigmentRuleID";
    private static final String RECORD_ID = "theRecordID";
    private static final Document INPUT_DATA = mock(Document.class, RETURNS_DEEP_STUBS);
    private static final String DELETE_PARAMETERS = "theDeleteParameters";
    private static final String EXTERNAL_ID_FIELD = "theExternalIDField";
    private static final String QUERY_STRING = "theQueryString";
    private static final String NEXT_URL = "theNextURL";
    private static final long PAGE_SIZE = 42L;
    private static final long CONTENT_LENGTH = 0L;
    private static final String EXTERNAL_ID_VALUE = "theExternalIDValue";

    private RequestBuilder _builderMock;
    private RequestExecutor _executorMock;
    private RequestHandler _requestHandler;

    @BeforeEach
    void setup() {
        _builderMock = mock(RequestBuilder.class, Mockito.RETURNS_DEEP_STUBS);
        _executorMock = mock(RequestExecutor.class, Mockito.RETURNS_DEEP_STUBS);
        _requestHandler = new RequestHandler(_builderMock, _executorMock);
    }

    @Test
    void executeGetObjects() {
        ClassicHttpResponse result = _requestHandler.executeGetObjects();

        assertNotNull(result);
        verify(_builderMock, times(1)).getSObjects();
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void getBulkSuccessResult() {
        ClassicHttpResponse result = _requestHandler.getBulkSuccessResult(JOB_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).getSuccessResult(JOB_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void getBulkQueryStatus() {
        ClassicHttpResponse result = _requestHandler.getBulkQueryStatus(JOB_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).getQueryJobStatus(JOB_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeCreate() {
        ClassicHttpResponse result = _requestHandler.executeCreate(OBJECT_NAME, EMPTY_STREAM, CONTENT_LENGTH,
                ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).createRecord(OBJECT_NAME, EMPTY_STREAM, CONTENT_LENGTH, ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeCreateTree() {
        ClassicHttpResponse result = _requestHandler.executeCreateTree(OBJECT_NAME, EMPTY_STREAM, CONTENT_LENGTH,
                ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).createTree(OBJECT_NAME, EMPTY_STREAM, CONTENT_LENGTH, ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeUpsert() {
        ClassicHttpResponse result = _requestHandler.executeUpsert(OBJECT_NAME, EXTERNAL_ID_FIELD, EXTERNAL_ID_VALUE,
                EMPTY_STREAM, ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).upsertRecord(eq(OBJECT_NAME), eq(EXTERNAL_ID_FIELD), eq(EXTERNAL_ID_VALUE),
                any(), eq(ASSIGMENT_RULE_ID));
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeDelete() {
        _requestHandler.executeDelete(OBJECT_NAME, RECORD_ID);

        verify(_builderMock, times(1)).deleteRecord(OBJECT_NAME, RECORD_ID);
        verify(_executorMock, times(1)).executeVoid(any(), any());
    }

    @Test
    void executeUpdate() {
        _requestHandler.executeUpdate(OBJECT_NAME, RECORD_ID, INPUT_DATA, ASSIGMENT_RULE_ID);

        verify(_builderMock, times(1)).updateRecord(eq(OBJECT_NAME), eq(RECORD_ID), any(), eq(ASSIGMENT_RULE_ID));
        verify(_executorMock, times(1)).executeVoid(any(), any());
    }

    @Test
    void uploadData() {
        _requestHandler.uploadData(JOB_ID, EMPTY_STREAM, CONTENT_LENGTH);

        verify(_builderMock, times(1)).uploadBatch(JOB_ID, EMPTY_STREAM, CONTENT_LENGTH);
        verify(_executorMock, times(1)).executeVoid(any(), any());
    }

    @Test
    void finishUpload() {
        _requestHandler.finishUpload(JOB_ID);

        verify(_builderMock, times(1)).finishBatch(JOB_ID);
        verify(_executorMock, times(1)).executeVoid(any(), any());
    }

    @Test
    void executeGetFields() {
        ClassicHttpResponse result = _requestHandler.executeGetFields(OBJECT_NAME);

        assertNotNull(result);
        verify(_builderMock, times(1)).describeSObject(OBJECT_NAME);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeSOQLQuery() {
        ClassicHttpResponse result = _requestHandler.executeSOQLQuery(QUERY_STRING, PAGE_SIZE);

        assertNotNull(result);
        verify(_builderMock, times(1)).querySOQL(QUERY_STRING, PAGE_SIZE);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void getBulkStatus() {
        ClassicHttpResponse result = _requestHandler.getBulkStatus(JOB_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).getJobStatus(JOB_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void testConnection() {
        _requestHandler.testConnection();

        verify(_builderMock, times(1)).testConnection();
        verify(_executorMock, times(1)).executeVoid(any(), any());
    }

    @Test
    void getBulkQueryResult() {
        ClassicHttpResponse result = _requestHandler.getBulkQueryResult(JOB_ID, NEXT_URL, PAGE_SIZE);

        assertNotNull(result);
        verify(_builderMock, times(1)).getBulkQueryResult(JOB_ID, NEXT_URL, PAGE_SIZE);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void getFailedResult() {
        ClassicHttpResponse result = _requestHandler.getFailedResult(JOB_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).getFailedResult(JOB_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeSOQLQueryAll() {
        ClassicHttpResponse result = _requestHandler.executeSOQLQueryAll(QUERY_STRING, PAGE_SIZE);

        assertNotNull(result);
        verify(_builderMock, times(1)).buildSOQLQueryAll(QUERY_STRING, PAGE_SIZE);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void createBulkV2Job() {
        ClassicHttpResponse result = _requestHandler.createBulkV2Job();

        assertNotNull(result);
        verify(_builderMock, times(1)).buildJob();
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeNextPageQuery() {
        ClassicHttpResponse result = _requestHandler.executeNextPageQuery(NEXT_URL, PAGE_SIZE);

        assertNotNull(result);
        verify(_builderMock, times(1)).queryNextPage(NEXT_URL, PAGE_SIZE);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void createBulkV2QueryJob() {
        ClassicHttpResponse result = _requestHandler.createBulkV2QueryJob(QUERY_STRING);

        assertNotNull(result);
        verify(_builderMock, times(1)).buildQueryJob(QUERY_STRING);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void createCompositeCollection() {
        ClassicHttpResponse result = _requestHandler.createCompositeCollection(EMPTY_STREAM, ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).createCompositeCollection(EMPTY_STREAM, ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void updateCompositeCollection() {
        ClassicHttpResponse result = _requestHandler.updateCompositeCollection(EMPTY_STREAM, ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).updateCompositeCollection(EMPTY_STREAM, ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void upsertCompositeCollection() {
        ClassicHttpResponse result = _requestHandler.upsertCompositeCollection(OBJECT_NAME, EXTERNAL_ID_FIELD,
                EMPTY_STREAM, ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).upsertCompositeCollection(OBJECT_NAME, EXTERNAL_ID_FIELD, EMPTY_STREAM,
                ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void deleteCompositeCollection() {
        ClassicHttpResponse result = _requestHandler.deleteCompositeCollection(DELETE_PARAMETERS, true);

        assertNotNull(result);
        verify(_builderMock, times(1)).deleteCompositeCollection(DELETE_PARAMETERS, true);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }

    @Test
    void executeUpdateReturnUpdatedRecord() {
        ClassicHttpResponse result = _requestHandler.executeUpdateReturnUpdatedRecord(OBJECT_NAME, RECORD_ID,
                INPUT_DATA, ASSIGMENT_RULE_ID);

        assertNotNull(result);
        verify(_builderMock, times(1)).updateReturnUpdatedRecord(OBJECT_NAME, RECORD_ID, INPUT_DATA, ASSIGMENT_RULE_ID);
        verify(_executorMock, times(1)).executeStream(any(), any());
    }
}
