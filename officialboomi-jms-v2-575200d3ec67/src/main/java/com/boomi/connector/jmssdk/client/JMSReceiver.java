// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import javax.jms.Message;

import java.io.Closeable;

/**
 * Common abstraction for receiving messages from a JMS Service regardless of the underling JMS version
 */
public interface JMSReceiver extends Closeable {

    /**
     * Get a message from the configured destination without any timeout. If no message is available at this time,
     * {@code null} is immediately returned
     *
     * @return a {@link Message} or {@code null} if the destination is empty at this time
     */
    Message receiveNoWait();

    /**
     * Get a message from the configured destination. This method blocks until a message is retrieved.
     *
     * @return a {@link Message}
     */
    Message receive();

    /**
     * Get a message from the configured destination. If the timeout is exhausted before receiving a message, {@code
     * null} is returned.
     *
     * @param timeout how long this method will block attempting to retrieve a {@link Message}
     * @return a {@link Message} or {@code null} if the timeout is exhausted before receiving one
     */
    Message receive(long timeout);

    /**
     * Commit the session or context used to retrieve the messages. If transactions are disabled, this method does not
     * perform a commit.
     */
    void commit();
}
