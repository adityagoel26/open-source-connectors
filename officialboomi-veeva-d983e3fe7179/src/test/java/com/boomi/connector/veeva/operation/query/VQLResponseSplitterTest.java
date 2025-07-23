// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.fasterxml.jackson.core.JsonToken;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

class VQLResponseSplitterTest {

    private static final String NEXT_PAGE_ELEMENT_PATH =
            "http://hapi.fhir.org/baseR4?_getpages=4397d8e6-4cf0-47f2-b20d-80cbf6c8aa59&_getpagesoffset=20&_count=20"
                    + "&_pretty=true&_bundletype=searchset";

    private static final String RESPONSE =
            "{\"responseStatus\":\"SUCCESS\",\"responseDetails\":{\"pagesize\":1000," + "\"pageoffset\":0,\n"
                    + "\"size\":2,\"total\":2},\"data\":[{\"id\":1,\"name__v\":\"Trial Master File Plan_1\"},\n"
                    + "{\"id\":2,\"name__v\":\"Trial Master File Plan_2\"}]}";
    VQLResponseSplitter _splitter;

    @BeforeEach
    void setup() throws IOException {
        PayloadMetadata metadata = new SimplePayloadMetadata();
        try (InputStream is = new ByteArrayInputStream(RESPONSE.getBytes(StandardCharsets.UTF_8))) {
            _splitter = new VQLResponseSplitter(is, "/data/*", NEXT_PAGE_ELEMENT_PATH, metadata);
        }
    }

    @Test
    void findNextNodeStartTest() throws IOException {
        Assertions.assertEquals(JsonToken.START_OBJECT, _splitter.findNextNodeStart());
    }

    @Test
    void hasErrorTest() {
        Assertions.assertFalse(_splitter.hasError());
    }
}
