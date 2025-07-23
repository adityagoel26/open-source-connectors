// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.connector.jmssdk.client.OracleAQAdapter;
import com.boomi.connector.jmssdk.operations.json.MapMessageParser;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.jmssdk.util.TextXMLPayloadValidator;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.io.IOException;
import java.io.InputStream;

/**
 * Static Factory to build an instance of the appropriate {@link Message} subtype needed.
 */
final class SendMessageFactory {

    private SendMessageFactory() {
    }

    /**
     * Instantiate and build the appropriate {@link Message} subtype.
     *
     * @param adapter         used to create the replyTo destination
     * @param sender          used to create the different subtypes of {@link Message}s
     * @param destinationType used to determine the {@link Message} subtype needed
     * @param document        used to extract the properties and payload to fill the created {@link Message}
     * @return an instance of {@link Message}
     */
    static Message createMessage(GenericJndiBaseAdapter adapter, JMSSender sender, DestinationType destinationType,
            ObjectData document, TargetDestination targetDestination) {
        Message message;
        switch (destinationType) {
            case TEXT_MESSAGE:
                TextMessage textMessage = sender.createTextMessage();
                message = buildTextMessage(textMessage, document);
                break;
            case TEXT_MESSAGE_XML:
                TextMessage textXmlMessage = sender.createTextMessage();
                message = buildTextXMLMessage(textXmlMessage, document);
                break;
            case BYTE_MESSAGE:
                BytesMessage bytesMessage = sender.createBytesMessage();
                message = buildBytesMessage(bytesMessage, document);
                break;
            case MAP_MESSAGE:
                MapMessage mapMessage = sender.createMapMessage();
                message = buildMapMessage(mapMessage, document);
                break;
            case ADT_MESSAGE:
                if (adapter instanceof OracleAQAdapter) {
                    message = ((OracleAQAdapter) adapter).createObjectMessage(document, targetDestination);
                } else {
                    throw new IllegalStateException("adapter does not support destination type " + destinationType);
                }
                break;
            default:
                throw new IllegalArgumentException("unknown destination type " + destinationType);
        }

        MessagePropertiesDelegate propertiesDelegate = new MessagePropertiesDelegate(message);
        DynamicPropertyMap operationProperties = document.getDynamicOperationProperties();

        propertiesDelegate.setCorrelationID(operationProperties.getProperty(JMSConstants.PROPERTY_CORRELATION_ID))
                .setPriority(operationProperties.getProperty(JMSConstants.PROPERTY_PRIORITY)).setType(
                        operationProperties.getProperty(JMSConstants.PROPERTY_MESSAGE_TYPE)).setReplyTo(
                        operationProperties.getProperty(JMSConstants.PROPERTY_REPLY_TO), adapter).setCustomProperties(
                        operationProperties.getCustomProperties(JMSConstants.PROPERTY_CUSTOM_OPERATION_PROPERTIES));

        return message;
    }

    private static Message buildMapMessage(MapMessage message, ObjectData document) {
        InputStream stream = null;
        try {
            stream = document.getData();
            MapMessageParser.fillMapMessage(stream, message);
        } finally {
            IOUtil.closeQuietly(stream);
        }

        return message;
    }

    private static Message buildBytesMessage(BytesMessage message, ObjectData document) {
        InputStream stream = null;
        try {
            stream = document.getData();
            long size = document.getDataSize();

            if (size > Integer.MAX_VALUE) {
                throw new ConnectorException("cannot process document, data too large");
            }

            byte[] bytes = new byte[(int) size];
            StreamUtil.readFully(stream, bytes);

            message.writeBytes(bytes);
        } catch (IOException | JMSException e) {
            throw new ConnectorException("error building byte message", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }

        return message;
    }

    private static Message buildTextMessage(TextMessage message, ObjectData document) {
        try {
            message.setText(writeToString(document.getData()));
        } catch (JMSException e) {
            throw new ConnectorException("error building text message", e);
        }

        return message;
    }

    private static Message buildTextXMLMessage(TextMessage message, ObjectData document) {

        InputStream stream = null;
        try {
            stream = TextXMLPayloadValidator.assertXMLContent(document.getData());
            String payload = writeToString(stream);

            message.setText(payload);
        } catch (JMSException e) {
            throw new ConnectorException("error building text XML message", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }

        return message;
    }

    private static String writeToString(InputStream stream) {
        try {
            return StreamUtil.toString(stream, StringUtil.UTF8_CHARSET);
        } catch (IOException e) {
            throw new ConnectorException("error parsing document", e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }
}
