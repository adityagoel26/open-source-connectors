// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.util.CollectionUtil;
import com.boomi.util.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Collections;
import java.util.Map;

/**
 * Metadata associated to JMS Send operation Messages
 */
class SendMessageMetadata {

    private static final ObjectMapper OBJECT_MAPPER = JSONUtil.getDefaultObjectMapper();

    private final Map<String, Object> _values;

    SendMessageMetadata() {
        _values = Collections.emptyMap();
    }

    SendMessageMetadata(String messageID, String destination, DestinationType destinationType) {
        _values = CollectionUtil.<String, Object>mapBuilder().put("messageId", messageID).put("destination",
                destination).put("destinationType", destinationType).finishImmutable();
    }

    /**
     * Use the metadata present in the instance to build a JSON {@link Payload}
     *
     * @return a JSON payload with the Message Metadata
     */
    Payload toPayload() {
        try {
            return PayloadUtil.toPayload(OBJECT_MAPPER.writeValueAsString(_values));
        } catch (JsonProcessingException e) {
            throw new ConnectorException("error building output payload", e);
        }
    }
}
