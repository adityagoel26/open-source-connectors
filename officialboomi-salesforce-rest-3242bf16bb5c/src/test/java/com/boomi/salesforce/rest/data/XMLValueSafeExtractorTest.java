// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import com.boomi.util.IOUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.xml.stream.XMLStreamException;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class XMLValueSafeExtractorTest {

    private static final Integer LIMIT = 100000;
    private static final String COMPOSITE_RESPONSE_XML = "compositeResponse.xml";
    private static final String COMPOSITE_RESPONSE_LONG_CONTENT_XML = "compositeResponse_LongContent.xml";

    @Test
    public void shouldSplitCompositeResponse() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(COMPOSITE_RESPONSE_XML);
        XMLValueSafeExtractor splitter = null;
        try {
            splitter = buildXmlValueSafeExtractor(xmlInput);
            assertTrue(splitter.containsKey("message"));
            assertEquals("Account ID: id value of incorrect type: 24", splitter.getValue("message"));
        } finally {
            IOUtil.closeQuietly(xmlInput, splitter);
        }
    }

    @Test
    public void shouldCropLargeContent() throws Exception {
        InputStream xmlInput = SFRestTestUtil.getContent(COMPOSITE_RESPONSE_LONG_CONTENT_XML);
        XMLValueSafeExtractor splitter = null;
        InputStream result = null;
        try {
            splitter = buildXmlValueSafeExtractor(xmlInput);
            assertTrue(splitter.containsKey("message"));
            assertTrue(splitter.getValue("message").length() <= LIMIT);

            result = SFRestTestUtil.getContent(COMPOSITE_RESPONSE_LONG_CONTENT_XML);
            assertTrue(SFRestTestUtil.getElementFromStream(result, "message").length() >= LIMIT);
        } finally {
            IOUtil.closeQuietly(xmlInput, splitter, result);
        }
    }

    private static XMLValueSafeExtractor buildXmlValueSafeExtractor(InputStream xmlInput)
            throws IOException, XMLStreamException {
        ClassicHttpResponse response = mock(ClassicHttpResponse.class, Mockito.RETURNS_DEEP_STUBS);
        when(response.getEntity().getContent()).thenReturn(xmlInput);
        return new XMLValueSafeExtractor(response, Collections.singletonList("message"));
    }
}
