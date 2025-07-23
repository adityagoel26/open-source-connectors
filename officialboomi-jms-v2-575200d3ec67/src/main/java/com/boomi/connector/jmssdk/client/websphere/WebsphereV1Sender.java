// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.jmssdk.client.JMSV1Sender;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.LogUtil;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A wrapper class for {@link JMSV1Sender} that sets the message priority to the {@link javax.jms.MessageProducer}
 * before sending a message to IBM Websphere.
 */
public class WebsphereV1Sender extends JMSV1Sender {

    private static final Logger LOG = LogUtil.getLogger(WebsphereV1Sender.class);

    WebsphereV1Sender(Session jmsSession) {
        super(jmsSession);
    }

    @Override
    public void send(Destination destination, Message message, Long timeToLive) {
        try {
            _jmsProducer.setPriority(Utils.getMessagePriority(message));
        } catch (JMSException e) {
            LOG.log(Level.WARNING, "cannot set message priority", e);
        }

        super.send(destination, message, timeToLive);
    }
}
