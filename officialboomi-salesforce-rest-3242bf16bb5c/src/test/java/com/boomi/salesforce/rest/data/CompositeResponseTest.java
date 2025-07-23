// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import com.boomi.util.TempOutputStream;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.Test;

import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeResponseTest {
    private static final Integer LIMIT = 100001;
    private static final String COMPOSITE_RESPONSE_XML = "compositeResponse.xml";
    private static final String COMPOSITE_RESPONSE_LONG_CONTENT_XML = "compositeResponse_LongContent.xml";

    @Test
    public void shouldSplitCompositeResponse() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(COMPOSITE_RESPONSE_XML);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(xmlInput);
        CompositeResponseSplitter splitter = new CompositeResponseSplitter(response);
        int counter = 0;
        while (splitter.hasNext()) {
            splitter.getNextResult();
            if (counter == 0 || counter == 1) {
                assertTrue(splitter.wasSuccess());
            } else {
                assertFalse(splitter.wasSuccess());
                assertEquals("Account ID: id value of incorrect type: 24 MALFORMED_ID", splitter.getErrorMessage());
            }
            counter++;
        }
    }

    @Test
    public void shouldCropLargeContent() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(COMPOSITE_RESPONSE_LONG_CONTENT_XML);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(xmlInput);
        CompositeResponseSplitter splitter = new CompositeResponseSplitter(response);
        TempOutputStream out;
        int counter = 0;
        while (splitter.hasNext()) {
            out = splitter.getNextResult();
            if (counter == 0 || counter == 1) {
                assertTrue(splitter.wasSuccess());
            } else {
                assertFalse(splitter.wasSuccess());
                assertTrue(splitter.getErrorMessage().length() <= LIMIT);
            }
            InputStream in = out.toInputStream();
            in.close();
            counter++;
        }
    }
}