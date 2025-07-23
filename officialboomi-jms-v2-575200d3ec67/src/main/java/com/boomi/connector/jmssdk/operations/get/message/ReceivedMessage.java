// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.jmssdk.operations.get.message;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.PayloadUtil;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.OracleAQAdapter;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wrapper class for JMS {@link Message}s that provides a {@link #toPayload(PayloadMetadata)} method in order to
 * generate the {@link Payload} representation of the message.
 * <p>
 * The factory method {@link #wrapMessage(Message, DestinationType)} is available for obtaining the appropriate subtype
 * instance for a given {@link DestinationType}.
 * <p>
 * When dealing with a null {@link Message}, the method {@link #emptyMessage()} can be invoked to obtain an empty
 * implementation of this class.
 */
public abstract class ReceivedMessage {

    private static final Logger LOG = LogUtil.getLogger(ReceivedMessage.class);
    protected final Message _message;
    private final Destination _destination;

    ReceivedMessage(Message message) {
        _message = message;
        try {
            _destination = _message.getJMSDestination();
        } catch (JMSException e) {
            throw new ConnectorException("cannot get Destination from received message", e);
        }
    }

    private ReceivedMessage() {
        _message = null;
        _destination = null;
    }

    /**
     * Create an instance of the appropriate subtype of {@link ReceivedMessage} wrapping the given {@link Message}
     *
     * @param message         the non-null message
     * @param destinationType the destination type
     * @return the {@link ReceivedMessage} wrapping the given message
     */
    public static ReceivedMessage wrapMessage(Message message, DestinationType destinationType,
            GenericJndiBaseAdapter adapter, TargetDestination targetDestination) {
        switch (destinationType) {
            case TEXT_MESSAGE:
                return new ReceivedTextMessage(message);
            case BYTE_MESSAGE:
                return new ReceivedBytesMessage(message);
            case MAP_MESSAGE:
                return new ReceivedMapMessage(message);
            case ADT_MESSAGE:
                if (adapter instanceof OracleAQAdapter) {
                    return new ReceivedADTMessage(message, (OracleAQAdapter) adapter, targetDestination);
                } else {
                    throw new IllegalStateException("adapter does not support destination type " + destinationType);
                }
            default:
                throw new IllegalArgumentException("unknown destination type " + destinationType);
        }
    }

    /**
     * Create an empty instance of {@link ReceivedMessage}
     *
     * @return an empty {@link ReceivedMessage}
     */
    public static ReceivedMessage emptyMessage() {
        return EMPTY_MESSAGE;
    }

    /**
     * Create a {@link Payload} from the content of the message contained by this instance.
     * <p>
     * This method should be invoked only when {@link #hasMessage()} returns {@code true}
     *
     * @param metadata the metadata object to hold the attributes of the message
     * @return the Payload from the contained message
     */
    public Payload toPayload(PayloadMetadata metadata) {
        boolean isSuccess = false;
        InputStream stream = null;
        try {
            stream = getMessagePayload();
            fillMetadata(metadata);
            Payload payload = PayloadUtil.toPayload(stream, metadata);
            isSuccess = true;
            return payload;
        } finally {
            if (!isSuccess) {
                // the stream is closed regardless of being a ByteArrayInputStream to prevent
                // leaving it open if another implementation is used in the future
                IOUtil.closeQuietly(stream);
            }
        }
    }

    abstract DestinationType getDestinationType();

    /**
     * Verify if this instance contains a {@link Message} or not.
     *
     * @return {@code true} if a message is present, {@code false} otherwise.
     */
    public boolean hasMessage() {
        return _message != null;
    }

    void fillMetadata(PayloadMetadata metadata) {
        try {
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_CORRELATION_ID, _message.getJMSCorrelationID());
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_MESSAGE_ID, _message.getJMSMessageID());
            if(_destination !=  null) {
                metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_DESTINATION, _destination.toString());
            }
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_MESSAGE_TYPE, getDestinationType().name());
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_PRIORITY,
                    String.valueOf(_message.getJMSPriority()));
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_EXPIRATION_TIME,
                    String.valueOf(_message.getJMSExpiration()));
            setJMSReplyToInTrackedProperty(metadata);
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_MESSAGE_CLASS, _message.getClass().toString());
            metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_REDELIVERED,
                    String.valueOf(_message.getJMSRedelivered()));

            Iterable<String> propertyKeys = Utils.toIterable(_message.getPropertyNames());
            Map<String, String> propertyGroup = new HashMap<>();
            for (String key : propertyKeys) {
                propertyGroup.put(key, String.valueOf(_message.getObjectProperty(key)));
            }

            metadata.setTrackedGroupProperties(JMSConstants.TRACKED_PROPERTY_GROUP, propertyGroup);
        } catch (JMSException e) {
            throw new ConnectorException("an error happened building the payload metadata", e);
        }
    }

    private void setJMSReplyToInTrackedProperty(PayloadMetadata metadata) {
        String jmsReplyTo = StringUtil.EMPTY_STRING;
        try {
            jmsReplyTo = StringUtil.toString(_message.getJMSReplyTo());
        } catch (JMSException e) {
            LOG.log(Level.WARNING, "JMSReplyto cannot be set in the tracked property", e);
        }
        metadata.setTrackedProperty(JMSConstants.TRACKED_PROPERTY_REPLY_TO, jmsReplyTo);
    }

    abstract InputStream getMessagePayload();

    private static final ReceivedMessage EMPTY_MESSAGE = new ReceivedMessage() {
        @Override
        public Payload toPayload(PayloadMetadata metadata) {
            throw new UnsupportedOperationException();
        }

        @Override
        DestinationType getDestinationType() {
            throw new UnsupportedOperationException();
        }

        @Override
        InputStream getMessagePayload() {
            throw new UnsupportedOperationException();
        }
    };
}
