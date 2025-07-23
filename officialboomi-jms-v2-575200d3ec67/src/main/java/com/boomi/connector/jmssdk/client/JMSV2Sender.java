// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import javax.jms.BytesMessage;
import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Concrete implementation of {@link JMSSender} for JMS version 2.0
 */
public class JMSV2Sender implements JMSSender {

    private static final Logger LOG = LogUtil.getLogger(JMSV2Sender.class);

    private final JMSContext _jmsContext;
    protected final JMSProducer _jmsProducer;

    protected JMSV2Sender(JMSContext jmsContext) {
        _jmsContext = jmsContext;
        boolean isSuccess = false;
        try {
            _jmsProducer = _jmsContext.createProducer();
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                IOUtil.closeQuietly(_jmsContext);
            }
        }
    }

    @Override
    public void send(Destination destination, Message message, Long timeToLive) {
        _jmsProducer.setTimeToLive(timeToLive);
        _jmsProducer.send(destination, message);
    }

    @Override
    public void commit() {
        _jmsContext.commit();
    }

    @Override
    public void rollback() {
        try {
            _jmsContext.rollback();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "cannot rollback transaction", e);
        }
    }

    @Override
    public TextMessage createTextMessage() {
        return _jmsContext.createTextMessage();
    }

    @Override
    public MapMessage createMapMessage() {
        return _jmsContext.createMapMessage();
    }

    @Override
    public BytesMessage createBytesMessage() {
        return _jmsContext.createBytesMessage();
    }

    @Override
    public void close() {
        IOUtil.closeQuietly(_jmsContext);
    }
}
