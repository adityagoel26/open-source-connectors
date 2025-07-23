// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;

import javax.jms.Message;

/**
 * Concrete implementation of {@link ReceiveStrategy} for retrieving messages from JMS until a timeout is exhausted or a
 * message count is reached. Each invocation of {@link #receiveMessage(JMSReceiver)} blocks until getting a {@link Message} or
 * exhausting the remaining timeout.
 * <p>
 * For this implementation, {@link #shouldContinue()} returns {@code true} until the configured number of messages is
 * reached or the timeout is exhausted.
 */
class ReceiveLimitedWithTimeoutStrategy implements ReceiveStrategy {

    private final long _maxNumberOfMessages;
    private final long _endTime;

    private long _messageCount;

    ReceiveLimitedWithTimeoutStrategy(long timeout, long maxNumberOfMessages) {
        _maxNumberOfMessages = maxNumberOfMessages;
        _endTime = System.currentTimeMillis() + timeout;
    }

    @Override
    public Message receiveMessage(JMSReceiver receiver) {
        long timeout = Math.max(0L, _endTime - System.currentTimeMillis());
        Message message = receiver.receive(timeout);
        _messageCount++;
        return message;
    }

    @Override
    public boolean shouldContinue() {
        return (_messageCount < _maxNumberOfMessages) && (System.currentTimeMillis() < _endTime);
    }
}
