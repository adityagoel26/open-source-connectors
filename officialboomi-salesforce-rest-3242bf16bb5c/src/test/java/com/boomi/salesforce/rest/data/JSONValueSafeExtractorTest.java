// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JSONValueSafeExtractorTest {
    private static final String SAMPLE_JSON = "sample.json";

    @Test
    public void shouldCropLargeContent() throws Exception {
        InputStream jsonInput = SFRestTestUtil.getContent(SAMPLE_JSON);

        ClassicHttpResponse response = mock(ClassicHttpResponse.class);
        HttpEntity entity = mock(HttpEntity.class);
        when(response.getEntity()).thenReturn(entity);
        when(entity.getContent()).thenReturn(jsonInput);
        List<String> list = Arrays.asList("val", "message", "fields");
        JSONValueSafeExtractor splitter = new JSONValueSafeExtractor(response, list);

        assertEquals("false", splitter.getValue("val"));
        assertEquals("Id", splitter.getValue("fields"));
        assertEquals("test message", splitter.getValue("message"));
        for (String key : list) {
            assertTrue(splitter.containsKey(key));
        }

        jsonInput.close();
        splitter.close();
    }
}
