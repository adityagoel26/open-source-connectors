// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.util;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

public class LogUtilsTest {

    @Test
    void testRequestMethodAndUriMessageRequestBuilder() {
        RequestBuilder requestBuilder = RequestBuilder.get().setUri("https://example.com");
        Supplier<String> stringSupplier = LogUtils.requestMethodAndUriMessage(requestBuilder);
        Assertions.assertEquals("GET : https://example.com", stringSupplier.get());
    }

    @Test
    void testRequestMethodAndUriMessageRequest() {
        HttpUriRequest request = new HttpGet("https://example.com");
        Supplier<String> stringSupplier = LogUtils.requestMethodAndUriMessage(request);
        Assertions.assertEquals("GET : https://example.com", stringSupplier.get());
    }
}
