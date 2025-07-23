// Copyright (c) 2025 Boomi, LP.
package com.boomi.connector.veeva.util;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutionUtilsTest {

    @Test
    void testSetCustomHeaders() {
        HttpRequestBase requestBase = new HttpPost();
        // Create a map with both empty and non-empty headers
        Map<String, String> testHeaders = new HashMap<>();
        testHeaders.put("validHeader", "value");
        testHeaders.put("emptyHeader", "");
        testHeaders.put("nullHeader", null);

        ExecutionUtils.setCustomHeaders(requestBase, testHeaders);

        // Get all headers as Header[] array
        Header[] headers = requestBase.getAllHeaders();

        // Convert to Map<String, String>
        Map<String, String> headerMap = Arrays.stream(headers).collect(
                Collectors.toMap(Header::getName, Header::getValue,
                        // In case of duplicate headers, keep the last value
                        (existing, replacement) -> replacement));

        // Verify that the empty and null headers are not added to the request
        Assertions.assertEquals(1, headerMap.size());
        Assertions.assertTrue(headerMap.containsKey("validHeader"));
        Assertions.assertEquals("value", headerMap.get("validHeader"));
    }
}
