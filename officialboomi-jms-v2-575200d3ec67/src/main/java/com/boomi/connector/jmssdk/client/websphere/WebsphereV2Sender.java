// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.jmssdk.client.JMSV2Sender;
import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.Message;

/**
 * A wrapper class for {@link JMSV2Sender} that sets the message priority to the {@link javax.jms.JMSProducer} before
 * sending a message to IBM Websphere.
 */
public class WebsphereV2Sender extends JMSV2Sender {

    WebsphereV2Sender(JMSContext jmsContext) {
        super(jmsContext);
    }

    @Override
    public void send(Destination destination, Message message, Long timeToLive) {
        _jmsProducer.setPriority(Utils.getMessagePriority(message));
        super.send(destination, message, timeToLive);
    }
}
