// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.operations.get.BytesMessageInputStream;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;

import java.io.InputStream;

/**
 * Concrete implementation of {@link ReceivedMessage} for {@link BytesMessage}
 */
class ReceivedBytesMessage extends ReceivedMessage {

    ReceivedBytesMessage(Message message) {
        super(message);
    }

    @Override
    InputStream getMessagePayload() {
        try {
            return new BytesMessageInputStream(getMessage());
        } catch (JMSException e) {
            throw new ConnectorException("error building payload for Bytes Message", e);
        }
    }

    @Override
    DestinationType getDestinationType() {
        return DestinationType.BYTE_MESSAGE;
    }

    private BytesMessage getMessage() {
        return (BytesMessage) _message;
    }
}
