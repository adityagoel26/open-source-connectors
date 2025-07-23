// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

import java.io.IOException;

/**
 * Concrete implementation of {@link JMSReceiver} for JMS version 1.1
 */
class JMSV1Receiver implements JMSReceiver {

    private static final String ERROR_MESSAGE_NOT_CREATE_JMS_CONSUMER = "cannot create JMS Message Consumer";
    private MessageConsumer _consumer;
    private final Session _session;
    private final boolean _transactionsDisabled;

    JMSV1Receiver(Session jmsSession, Destination destination, String messageSelector) {
        _session = jmsSession;
        boolean isSuccess = false;
        try {
            _transactionsDisabled = !_session.getTransacted();
            _consumer = jmsSession.createConsumer(destination, messageSelector);
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException(ERROR_MESSAGE_NOT_CREATE_JMS_CONSUMER, e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_session);
            }
        }
    }

    JMSV1Receiver(Session jmsSession, Destination destination, String subscriptionName, String messageSelector) {
        _session = jmsSession;
        boolean isSuccess = false;
        Topic topic = Utils.validateAndCast(destination, Topic.class);
        try {
            _transactionsDisabled = !_session.getTransacted();
            _consumer = jmsSession.createDurableSubscriber(topic, subscriptionName, messageSelector, false);
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException(ERROR_MESSAGE_NOT_CREATE_JMS_CONSUMER, e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_session);
            }
        }
    }

    JMSV1Receiver(Session jmsSession, MessageConsumer consumer) {
        _session = jmsSession;
        _consumer = consumer;
        boolean isSuccess = false;
        try {
            _transactionsDisabled = !_session.getTransacted();
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException(ERROR_MESSAGE_NOT_CREATE_JMS_CONSUMER, e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_session);
            }
        }
    }

    @Override
    public Message receiveNoWait() {
        try {
            return _consumer.receiveNoWait();
        } catch (JMSException e) {
            throw new ConnectorException("error receiving message without waiting", e);
        }
    }

    @Override
    public Message receive() {
        try {
            return _consumer.receive();
        } catch (JMSException e) {
            throw new ConnectorException("error receiving message without timeout", e);
        }
    }

    @Override
    public Message receive(long timeout) {
        try {
            return _consumer.receive(timeout);
        } catch (JMSException e) {
            throw new ConnectorException("error receiving message with timeout in milliseconds: " + timeout, e);
        }
    }

    @Override
    public void commit() {
        if (_transactionsDisabled) {
            return;
        }

        try {
            _session.commit();
        } catch (JMSException e) {
            throw new ConnectorException("error committing session", e);
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(_consumer);
        Utils.closeQuietly(_session);
    }
}
