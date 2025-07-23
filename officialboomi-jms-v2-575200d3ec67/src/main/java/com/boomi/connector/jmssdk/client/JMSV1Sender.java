// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.LogUtil;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concrete implementation of {@link JMSSender} for JMS version 1.1
 */
public class JMSV1Sender implements JMSSender {

    private static final Logger LOG = LogUtil.getLogger(JMSV1Sender.class);

    private static final Destination UNSET_DESTINATION = null;

    private final Session _jmsSession;
    protected final MessageProducer _jmsProducer;

    protected JMSV1Sender(Session jmsSession) {
        _jmsSession = jmsSession;
        boolean isSuccess = false;
        try {
            _jmsProducer = _jmsSession.createProducer(UNSET_DESTINATION);
            isSuccess = true;
        } catch (Exception e) {
            throw new ConnectorException("cannot create JMS Message Producer", e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(_jmsSession);
            }
        }
    }

    @Override
    public void send(Destination destination, Message message, Long timeToLive) {
        try {
            _jmsProducer.setTimeToLive(timeToLive);
            _jmsProducer.send(destination, message);
        } catch (JMSException e) {
            throw new ConnectorException("cannot send message", e);
        }
    }

    @Override
    public void commit() {
        try {
            _jmsSession.commit();
        } catch (JMSException e) {
            throw new ConnectorException("cannot commit transaction", e);
        }
    }

    @Override
    public void rollback() {
        try {
            _jmsSession.rollback();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot rollback transaction", e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        try {
            return _jmsSession.createTextMessage();
        } catch (JMSException e) {
            throw new ConnectorException("cannot create text message", e);
        }
    }

    @Override
    public MapMessage createMapMessage() {
        try {
            return _jmsSession.createMapMessage();
        } catch (JMSException e) {
            throw new ConnectorException("cannot create map message", e);
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        try {
            return _jmsSession.createBytesMessage();
        } catch (JMSException e) {
            throw new ConnectorException("cannot create bytes message", e);
        }
    }

    @Override
    public void close() {
        Utils.closeQuietly(_jmsProducer);
        Utils.closeQuietly(_jmsSession);
    }
}
