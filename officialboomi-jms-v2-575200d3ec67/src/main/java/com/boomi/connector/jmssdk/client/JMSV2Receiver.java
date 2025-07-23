// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.IOUtil;

import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.Message;
import javax.jms.Topic;

/**
 * Concrete implementation of {@link JMSReceiver} for JMS version 2.0
 */
class JMSV2Receiver implements JMSReceiver {

    private final JMSConsumer _consumer;
    private final JMSContext _context;
    private final boolean _transactionsEnabled;

    JMSV2Receiver(JMSContext jmsContext, Destination destination, String messageSelector) {
        _context = jmsContext;

        boolean isSuccess = false;
        try {
            _transactionsEnabled = _context.getTransacted();
            _consumer = jmsContext.createConsumer(destination, messageSelector);
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(_context);
            }
        }
    }

    JMSV2Receiver(JMSContext jmsContext, Destination destination, String subscriptionName, String messageSelector) {
        _context = jmsContext;
        _transactionsEnabled = _context.getTransacted();
        Topic topic = Utils.validateAndCast(destination, Topic.class);
        _consumer = jmsContext.createDurableConsumer(topic, subscriptionName, messageSelector, false);
    }

    @Override
    public Message receiveNoWait() {
        return _consumer.receiveNoWait();
    }

    @Override
    public Message receive() {
        return _consumer.receive();
    }

    @Override
    public Message receive(long timeout) {
        return _consumer.receive(timeout);
    }

    @Override
    public void commit() {
        if (_transactionsEnabled) {
            _context.commit();
        }
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_consumer, _context);
    }
}
