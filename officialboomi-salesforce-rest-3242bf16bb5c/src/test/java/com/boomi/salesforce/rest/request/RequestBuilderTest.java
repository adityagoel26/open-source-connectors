// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.request;

import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.util.StreamUtil;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ProtocolException;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class RequestBuilderTest {

    @Test
    public void upsertRecordRequestForInsertShouldBeAPost() throws ProtocolException {
        SFURIBuilder sfuriBuilder = mock(SFURIBuilder.class);
        OperationProperties operationProperties = mock(OperationProperties.class);
        RequestBuilder builder = new RequestBuilder(sfuriBuilder, operationProperties);

        // empty ID value means it should build the request to insert a new record
        String externalIdValue = "";

        String sObject = "theObjectType";
        String externalIdField = "externalID";
        InputStream inputData = StreamUtil.EMPTY_STREAM;
        String ruleID = "theRuleID";

        ClassicHttpRequest request = builder.upsertRecord(sObject, externalIdField, externalIdValue, inputData, ruleID);

        assertEquals("POST", request.getMethod());
        assertEquals(ruleID, request.getHeader("Sforce-Auto-Assign").getValue());
    }

    @Test
    public void upsertRecordRequestForUpdateShouldBeAPatch() throws ProtocolException {
        SFURIBuilder sfuriBuilder = mock(SFURIBuilder.class);
        OperationProperties operationProperties = mock(OperationProperties.class);
        RequestBuilder builder = new RequestBuilder(sfuriBuilder, operationProperties);

        // providing an ID value means it should build the request to update an existing record
        String externalIdValue = "someID";

        String sObject = "theObjectType";
        String externalIdField = "externalID";
        InputStream inputData = StreamUtil.EMPTY_STREAM;
        String ruleID = "theRuleID";

        ClassicHttpRequest request = builder.upsertRecord(sObject, externalIdField, externalIdValue, inputData, ruleID);

        assertEquals("PATCH", request.getMethod());
        assertEquals(ruleID, request.getHeader("Sforce-Auto-Assign").getValue());
    }
}
