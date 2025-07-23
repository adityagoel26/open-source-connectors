// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.salesforce.rest.testutil.SFRestTestUtil;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.core.JsonParser;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;

public class JSONUtilsTest {

    private static final String SAMPLE_JSON_FILE = "sample.json";

    @Test
    void getValuesMapSafely() throws IOException {
        ClassicHttpResponse response = buildHttpResponse();

        Map<String, String> result = JSONUtils.getValuesMapSafely(response, Arrays.asList("fields", "val", "message"));

        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals("Id", result.get("fields"));
        Assertions.assertEquals("false", result.get("val"));
        Assertions.assertEquals("test message", result.get("message"));
    }

    @Test
    void getValueSafely() throws IOException {
        ClassicHttpResponse response = buildHttpResponse();

        String result = JSONUtils.getValueSafely(response, "message");
        Assertions.assertEquals("test message", result);
    }

    /**
     * This test validates the {@link com.fasterxml.jackson.core.JsonFactory} available in {@link JSONUtils} does not
     * impose a length limit to JSON fields.
     * More details available here: https://boomii.atlassian.net/browse/CON-10174
     */
    @Test
    void jsonFactoryDoesNotApplyFieldLengthLimits() throws IOException {
        try (InputStream content = buildLargeJson()) {
            JsonParser parser = JSONUtils.getJsonFactory().createParser(content);
            while (parser.nextToken() != null) {
                // consume the stream
                parser.getValueAsString();
            }
        }
    }

    private static InputStream buildLargeJson() {
        StringBuilder largeValue = new StringBuilder();
        for (int i = 0; i < 30_000_000; i++) {
            largeValue.append("A");
        }

        String json = "{\"large\":\"%s\", \"example\":\"value\"}";

        return new ByteArrayInputStream(String.format(json, largeValue).getBytes(StringUtil.UTF8_CHARSET));
    }

    @Test
    void getValueSafelyWithKeyNotPresent() throws IOException {
        ClassicHttpResponse response = buildHttpResponse();

        String result = JSONUtils.getValueSafely(response, "unknown");
        Assertions.assertEquals("", result);
    }

    private static ClassicHttpResponse buildHttpResponse() throws IOException {
        InputStream content = SFRestTestUtil.getContent(SAMPLE_JSON_FILE);
        return new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return new InputStreamEntity(content, ContentType.APPLICATION_JSON);
            }
        };
    }
}
