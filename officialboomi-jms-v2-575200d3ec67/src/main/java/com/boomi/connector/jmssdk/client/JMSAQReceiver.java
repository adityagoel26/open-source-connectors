// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import javax.jms.Destination;
import javax.jms.Session;

public class JMSAQReceiver extends JMSV1Receiver {

    JMSAQReceiver(Session jmsSession, Destination destination, String messageSelector, boolean isProfileRequired) {
        super(jmsSession,
                AQConsumerFactory.createConsumer(jmsSession, destination, messageSelector, isProfileRequired));
    }

    JMSAQReceiver(Session jmsSession, Destination destination, String subscriptionName, String messageSelector,
            boolean isProfileRequired) {
        super(jmsSession,
                AQConsumerFactory.createDurableConsumer(jmsSession, destination, subscriptionName, messageSelector,
                        isProfileRequired));
    }
}
