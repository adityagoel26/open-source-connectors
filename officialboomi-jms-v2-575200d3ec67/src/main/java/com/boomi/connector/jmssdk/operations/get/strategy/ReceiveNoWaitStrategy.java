// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;

import javax.jms.Message;

/**
 * Concrete implementation of {@link ReceiveStrategy} for retrieving a single message from JMS. This implementation
 * immediately returns a {@link Message} or {@code null} if there isn't any available at the moment.
 * <p>
 * The method {@link #shouldContinue()} returns {@code true} until {@link #receiveMessage(JMSReceiver)} is called. After that,
 * the method always returns {@code false}.
 */
class ReceiveNoWaitStrategy implements ReceiveStrategy {

    private boolean _notExecutedYet = true;

    @Override
    public Message receiveMessage(JMSReceiver receiver) {
        _notExecutedYet = false;
        return receiver.receiveNoWait();
    }

    @Override
    public boolean shouldContinue() {
        return _notExecutedYet;
    }
}
