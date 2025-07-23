// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.util.TextXMLPayloadValidator;
import com.boomi.util.StringUtil;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * Concrete implementation of {@link ReceivedMessage} for {@link TextMessage}
 */
class ReceivedTextMessage extends ReceivedMessage {

    private DestinationType _destinationType = DestinationType.TEXT_MESSAGE;

    ReceivedTextMessage(Message message) {
        super(message);
    }

    @Override
    InputStream getMessagePayload() {
        try {
            String text = getMessage().getText();
            InputStream stream = new ByteArrayInputStream(text.getBytes(StringUtil.UTF8_CHARSET));
            if (TextXMLPayloadValidator.hasXMLContent(stream)) {
                _destinationType = DestinationType.TEXT_MESSAGE_XML;
            }
            return stream;
        } catch (JMSException e) {
            throw new ConnectorException("error building payload for Text Message", e);
        }
    }

    @Override
    DestinationType getDestinationType() {
        return _destinationType;
    }

    private TextMessage getMessage() {
        return (TextMessage) _message;
    }
}
