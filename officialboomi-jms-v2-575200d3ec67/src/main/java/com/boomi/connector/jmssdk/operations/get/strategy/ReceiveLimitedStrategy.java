// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;

import javax.jms.Message;

/**
 * Concrete implementation of {@link ReceiveStrategy} for retrieving a fixed amount of messages from JMS. Each
 * invocation of {@link #receiveMessage(JMSReceiver)} blocks for an unbound amount of time until returning a {@link Message}.
 * <p>
 * For this implementation, {@link #shouldContinue()} returns {@code true} until the configured number of messages is
 * reached.
 */
class ReceiveLimitedStrategy implements ReceiveStrategy {

    private final long _numberOfMessages;
    private long _messageCount;

    ReceiveLimitedStrategy(long numberOfMessages) {
        _numberOfMessages = numberOfMessages;
    }

    @Override
    public Message receiveMessage(JMSReceiver receiver) {
        Message message = receiver.receive();
        _messageCount++;
        return message;
    }

    @Override
    public boolean shouldContinue() {
        return _messageCount < _numberOfMessages;
    }
}
