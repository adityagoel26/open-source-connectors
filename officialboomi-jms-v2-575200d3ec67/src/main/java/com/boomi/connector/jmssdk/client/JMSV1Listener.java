// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerStrategy;
import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

public class JMSV1Listener implements JMSListener {

    private static final String JMS_MESSAGE_CONSUMER_ERROR = "Cannot create JMS Message Consumer";

    private MessageConsumer _consumer;
    private final Session _session;
    private final boolean _isTransacted;
    private boolean _isTopic;

    JMSV1Listener(Session jmsSession, Destination destination, String messageSelector) {
        _session = jmsSession;
        boolean isSuccess = false;
        try {
            _consumer = jmsSession.createConsumer(destination, messageSelector);
            _isTransacted = _session.getTransacted();
            _isTopic = destination instanceof Topic;
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException(JMS_MESSAGE_CONSUMER_ERROR, e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_session);
            }
        }
    }

    JMSV1Listener(Session jmsSession, Destination destination, String subscriptionName, String messageSelector) {
        _session = jmsSession;
        Topic topic = Utils.validateAndCast(destination, Topic.class);
        _isTopic = true;
        try {
            _consumer = jmsSession.createDurableSubscriber(topic, subscriptionName, messageSelector, false);
            _isTransacted = _session.getTransacted();
        } catch (Exception e) {
            throw new ConnectorException(JMS_MESSAGE_CONSUMER_ERROR, e);
        }
    }

    JMSV1Listener(Session jmsSession, MessageConsumer consumer) {
        _session = jmsSession;
        _consumer = consumer;
        boolean isSuccess = false;
        try {
            _isTransacted = _session.getTransacted();
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException("cannot create JMS Message Consumer", e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_session);
            }
        }
    }

    @Override
    public void subscribeConsumer(MessageListenerStrategy messageListenOperation) {
        try {
            _consumer.setMessageListener(messageListenOperation);
        } catch (JMSException e) {
            throw new ConnectorException("cannot set Listener", e);
        }
    }

    @Override
    public boolean isTransacted() {
        return _isTransacted;
    }

    @Override
    public void commit(Message message) {
        try {
            if (isTransacted()) {
                _session.commit();
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
                _session.rollback();
            } catch (JMSException e) {
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
        Utils.closeQuietly(_consumer);
        Utils.closeQuietly(_session);
    }
}
