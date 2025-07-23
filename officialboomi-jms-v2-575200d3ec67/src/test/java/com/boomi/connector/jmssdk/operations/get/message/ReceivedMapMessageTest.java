// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.Payload;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.testutil.SimplePayloadMetadata;
import com.boomi.connector.testutil.doubles.MapMessageDouble;
import com.boomi.util.CollectionUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Message;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ReceivedMapMessageTest {

    private static final ObjectMapper OBJECT_MAPPER = JSONUtil.getDefaultObjectMapper();

    @Test
    @SuppressWarnings("unchecked")
    public void toPayloadTest() throws IOException {
        Map<String, Object> expectedValues = CollectionUtil.<String, Object>mapBuilder() //
                .put("key1", "value1") //
                .put("key2", 2) //
                .put("key3", true) //
                .finishImmutable();
        String destination = "the destination";
        Message mapMessage = new MapMessageDouble(destination, expectedValues);
        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(mapMessage, DestinationType.MAP_MESSAGE,
                Mockito.mock(GenericJndiBaseAdapter.class), Mockito.mock(TargetDestination.class));
        SimplePayloadMetadata metadata = new SimplePayloadMetadata();

        Payload payload = receivedMessage.toPayload(metadata);

        InputStream stream = payload.readFrom();
        Map<String, Object> actualValues = OBJECT_MAPPER.readValue(stream, Map.class);
        assertEquals("value1", actualValues.get("key1"));
        assertEquals(2, actualValues.get("key2"));
        assertTrue("key3 should contain true", (Boolean) actualValues.get("key3"));

        Map<String, String> trackedProps = metadata.getTrackedProps();
        assertThat(trackedProps.get("destination"), is(destination));
        assertThat(trackedProps.get("message_type"), is("MAP_MESSAGE"));
    }
}
