// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerStrategy;

import javax.jms.Message;

import java.io.Closeable;

public interface JMSListener extends Closeable {

    /**
     * set message listener will receive  messages from destination.
     *
     * @param messageListenOperation @{@link MessageListenerStrategy}
     */
    void subscribeConsumer(MessageListenerStrategy messageListenOperation);

    /**
     * return true if the transaction mode is enabled
     *
     * @return @boolean
     */
    boolean isTransacted();

    /**
     * the method commits in the case that the transaction mode is enabled or do acknowledge message when the
     * transaction mode is disabled.
     *
     * @param message @{@link Message}
     */
    void commit(Message message);

    /**
     * the method rollback a transaction when it was enabled.
     */
    void rollbackIfNeeded();

    /**
     * return true when the destination is a topic
     *
     * @return @boolean
     */
    boolean isListeningFromTopic();
}
