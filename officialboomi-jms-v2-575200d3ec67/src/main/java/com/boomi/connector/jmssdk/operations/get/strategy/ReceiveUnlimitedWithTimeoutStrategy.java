// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;

import javax.jms.Message;

/**
 * Concrete implementation of {@link ReceiveStrategy} for retrieving messages from JMS until a timeout is exhausted.
 * Each invocation of {@link #receiveMessage(JMSReceiver)} blocks for, at most, the remaining timeout.
 * <p>
 * For this implementation, {@link #shouldContinue()} returns {@code true} until the configured timeout is exhausted.
 */
class ReceiveUnlimitedWithTimeoutStrategy implements ReceiveStrategy {

    private final long _endTime;

    ReceiveUnlimitedWithTimeoutStrategy(long timeout) {
        _endTime = System.currentTimeMillis() + timeout;
    }

    @Override
    public Message receiveMessage(JMSReceiver receiver) {
        long timeout = Math.max(0L, _endTime - System.currentTimeMillis());
        return receiver.receive(timeout);
    }

    @Override
    public boolean shouldContinue() {
        return System.currentTimeMillis() < _endTime;
    }
}
