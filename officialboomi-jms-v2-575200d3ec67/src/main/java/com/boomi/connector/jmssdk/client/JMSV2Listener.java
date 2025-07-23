// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerStrategy;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.IOUtil;

import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Topic;

public class JMSV2Listener implements JMSListener {

    private final JMSConsumer _consumer;
    private final JMSContext _context;
    private final boolean _transactionsEnabled;
    private final boolean _isTopic;

    JMSV2Listener(JMSContext jmsContext, Destination destination, String messageSelector) {
        _context = jmsContext;
        boolean isSuccess = false;
        try {
            _transactionsEnabled = _context.getTransacted();
            _consumer = jmsContext.createConsumer(destination, messageSelector);
            _isTopic = destination instanceof Topic;
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(_context);
            }
        }
    }

    JMSV2Listener(JMSContext jmsContext, Destination destination, String subscriptionName, String messageSelector) {
        _context = jmsContext;
        _transactionsEnabled = _context.getTransacted();
        _isTopic = true;
        Topic topic = Utils.validateAndCast(destination, Topic.class);
        _consumer = jmsContext.createSharedDurableConsumer(topic, subscriptionName, messageSelector);
    }

    @Override
    public void subscribeConsumer(MessageListenerStrategy messageListenOperation) {
        _consumer.setMessageListener(messageListenOperation);
    }

    @Override
    public boolean isTransacted() {
        return _transactionsEnabled;
    }

    @Override
    public void commit(Message message) {
        try {
            if (isTransacted()) {
                _context.commit();
            } else {
                message.acknowledge();
            }
        } catch (JMSException e) {
            throw new ConnectorException("Unable to commit JMS message", e);
        }
    }

    @Override
    public void rollbackIfNeeded() {
        if (isTransacted()) {
            try {
                _context.rollback();
            } catch (Exception e) {
                throw new ConnectorException("Unable to rollback JMS message", e);
            }
        }
    }

    @Override
    public boolean isListeningFromTopic() {
        return _isTopic;
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_consumer, _context);
    }
}
