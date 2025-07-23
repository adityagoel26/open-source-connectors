// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchUpdatedResponseSplitterTest {

    private static final String BATCH_UPDATE_SUCCESS_RESPONSE_XML = "batchUpdateSuccessResponse.xml";
    private static final String BATCH_UPDATE_FAIL_RESPONSE_XML = "batchUpdateFailResponse.xml";

    private static final String EXPECTED_PAYLOAD =
            "\n" + "                <Account type=\"Account\" url=\"/services/data/v50"
                    + ".0/sobjects/Account/0014K00000IEWy7QAH\">\n"
                    + "                    <Id>0014K00000IEWy7QAH</Id>\n"
                    + "                    <IsDeleted>false</IsDeleted>\n"
                    + "                    <Name>test batch 1</Name>\n"
                    + "                    <ParentId>0014K00000IEWy6QAH</ParentId>\n"
                    + "                    <PhotoUrl>/services/images/photo/0014K00000IEWy7QAH</PhotoUrl>\n"
                    + "                    <OwnerId>0054K000000mfx4QAA</OwnerId>\n"
                    + "                    <CreatedDate>2021-02-28T11:57:01.000Z</CreatedDate>\n"
                    + "                    <CreatedById>0054K000000mfx4QAA</CreatedById>\n"
                    + "                    <LastModifiedDate>2021-03-01T09:26:59.000Z</LastModifiedDate>\n"
                    + "                    <LastModifiedById>0054K000000mfx4QAA</LastModifiedById>\n"
                    + "                    <SystemModstamp>2021-03-01T09:26:59.000Z</SystemModstamp>\n"
                    + "                    <LastViewedDate>2021-03-01T09:26:59.000Z</LastViewedDate>\n"
                    + "                    <LastReferencedDate>2021-03-01T09:26:59.000Z</LastReferencedDate>\n"
                    + "                    <CleanStatus>Pending</CleanStatus>\n" + "                </Account>\n"
                    + "            ";

    @Test
    public void shouldSplitCompositeResponseWhenSuccess() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(BATCH_UPDATE_SUCCESS_RESPONSE_XML);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(xmlInput);

        BatchUpdatedResponseSplitter splitter = new BatchUpdatedResponseSplitter(response);

        BatchUpdatedResponse result = splitter.processResponse();

        assertFalse(result.hasErrors(), "the response should not inform any error");
        assertEquals("", result.getErrorMessage());

        InputStream in = null;
        try {
            in = result.getResponse();
            assertEquals(EXPECTED_PAYLOAD, StreamUtil.toString(in, StringUtil.UTF8_CHARSET));
        } finally {
            IOUtil.closeQuietly(in);
        }
    }

    @Test
    public void shouldSplitCompositeResponseWhenFailure() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(BATCH_UPDATE_FAIL_RESPONSE_XML);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(xmlInput);

        BatchUpdatedResponseSplitter splitter = new BatchUpdatedResponseSplitter(response);

        BatchUpdatedResponse result = splitter.processResponse();

        assertTrue(result.hasErrors(), "the splitter should inform errors");
        assertEquals("INVALID_CROSS_REFERENCE_KEY invalid cross reference id", result.getErrorMessage());

        InputStream in = null;
        try {
            in = result.getResponse();
            assertEquals("", StreamUtil.toString(in, StringUtil.UTF8_CHARSET),
                    "the payload should be empty when errors are present");
        } finally {
            IOUtil.closeQuietly(in);
        }
    }
}
