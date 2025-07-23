// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.io.Closeable;

/**
 * Common abstraction for sending messages to a JMS Service regardless of the underling JMS version
 */
public interface JMSSender extends Closeable {

    /**
     * Send the given {@link Message} to the indicated {@link Destination}
     *
     * @param destination where the message will be sent
     * @param message     the message to be sent
     * @param timeToLive  the time to live of messages that are sent using this JMSProducer. This is used to determine
     *                    the expiration time of a message and, it's in milliseconds.
     */
    void send(Destination destination, Message message, Long timeToLive);

    /**
     * Commit the current transaction
     */
    void commit();

    /**
     * Rollback the current transaction
     */
    void rollback();

    /**
     * Creates a {@link TextMessage}
     *
     * @return the {@link TextMessage}
     */
    TextMessage createTextMessage();

    /**
     * Creates a {@link MapMessage}
     *
     * @return the {@link MapMessage}
     */
    MapMessage createMapMessage();

    /**
     * Creates a {@link BytesMessage}
     *
     * @return the {@link BytesMessage}
     */
    BytesMessage createBytesMessage();
}
