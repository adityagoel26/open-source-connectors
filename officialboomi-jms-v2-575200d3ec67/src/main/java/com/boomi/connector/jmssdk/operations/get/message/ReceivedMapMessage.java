// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.operations.json.MapMessageParser;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;

import java.io.IOException;
import java.io.InputStream;

/**
 * Concrete implementation of {@link ReceivedMessage} for {@link MapMessage}
 */
class ReceivedMapMessage extends ReceivedMessage {

    ReceivedMapMessage(Message message) {
        super(message);
    }

    @Override
    InputStream getMessagePayload() {
        try {
            return MapMessageParser.buildJsonPayload(getMessage());
        } catch (JMSException | IOException e) {
            throw new ConnectorException("error building payload for Map Message", e);
        }
    }

    private MapMessage getMessage() {
        return (MapMessage) _message;
    }

    @Override
    DestinationType getDestinationType() {
        return DestinationType.MAP_MESSAGE;
    }
}
