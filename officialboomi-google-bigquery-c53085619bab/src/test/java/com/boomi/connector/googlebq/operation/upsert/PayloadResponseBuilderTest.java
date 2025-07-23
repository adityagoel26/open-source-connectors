// Copyright (c) 2022 Boomi, Inc.
package com.boomi.connector.googlebq.operation.upsert;

import com.boomi.connector.googlebq.operation.upsert.strategy.result.BaseStrategyResult;
import com.boomi.util.StringUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class PayloadResponseBuilderTest {

    @Test
    public void shouldCreatePayloadWithResultNode() {
        BaseStrategyResult result = mock(BaseStrategyResult.class);

        when(result.getCode()).thenReturn("code");
        when(result.getContent()).thenReturn(buildNode());
        PayloadResponseBuilder builder = new PayloadResponseBuilder();
        builder.withResult("node", result);

        assertNotNull(builder.toPayload());
        assertEquals("code", builder.getCode());
    }

    @Test
    public void shouldAddMessageWithErrorResultSuccessfully() {
        PayloadResponseBuilder builder = new PayloadResponseBuilder();
        builder.withException("test", "message");

        assertEquals("Error on step test message. ", builder.getMessage());
    }

    @Test
    public void shouldReturnEmptyWhenErrorMessageIsNotSet() {
        PayloadResponseBuilder builder = new PayloadResponseBuilder();
        assertEquals(StringUtil.EMPTY_STRING, builder.getMessage());
    }

    private static JsonNode buildNode() {
        ObjectNode node = JSONUtil.newObjectNode();
        node.put("field", "value");
        return node;
    }
}