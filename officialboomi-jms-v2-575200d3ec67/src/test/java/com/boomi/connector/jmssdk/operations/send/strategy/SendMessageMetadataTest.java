// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.Payload;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SendMessageMetadataTest {

    @Test
    public void toPayloadTest() throws IOException {
        SendMessageMetadata metadata = new SendMessageMetadata("the messageID", "the destination",
                DestinationType.MAP_MESSAGE);

        Payload payload = metadata.toPayload();

        JsonNode jsonPayload = toJson(payload);
        Assert.assertEquals("the messageID", jsonPayload.path("messageId").asText());
        Assert.assertEquals("the destination", jsonPayload.path("destination").asText());
        Assert.assertEquals("MAP_MESSAGE", jsonPayload.path("destinationType").asText());
    }

    private static JsonNode toJson(Payload payload) throws IOException {
        Assert.assertNotNull(payload);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        payload.writeTo(stream);
        return JSONUtil.getDefaultObjectMapper().readTree(stream.toString("utf8"));
    }
}
