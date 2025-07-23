// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import oracle.jms.AQjmsSession;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.operations.model.oracleaq.AQStructPayloadFactory;
import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.Topic;

class AQConsumerFactory {

    private static final String DEFAULT_TRANSFORMATION = null;
    private static final boolean DEFAULT_NO_LOCAL = false;

    private AQConsumerFactory() {
    }

    static MessageConsumer createConsumer(Session jmsSession, Destination destination, String messageSelector,
            boolean isProfileRequired) {
        MessageConsumer consumer = null;
        boolean isSuccess = false;
        try {
            if (isProfileRequired) {
                consumer = ((AQjmsSession) jmsSession).createConsumer(destination, messageSelector,
                        new AQStructPayloadFactory(), DEFAULT_TRANSFORMATION, DEFAULT_NO_LOCAL);
            } else {
                consumer = jmsSession.createConsumer(destination, messageSelector);
            }
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException("cannot create JMS Message Consumer", e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(jmsSession);
            }
        }
        return consumer;
    }

    static MessageConsumer createDurableConsumer(Session jmsSession, Destination destination, String subscriptionName,
            String messageSelector, boolean isProfileRequired) {
        Topic topic = Utils.validateAndCast(destination, Topic.class);
        MessageConsumer consumer = null;
        boolean isSuccess = false;
        try {
            if (isProfileRequired) {
                consumer = ((AQjmsSession) jmsSession).createDurableSubscriber(topic, subscriptionName, messageSelector,
                        DEFAULT_NO_LOCAL, new AQStructPayloadFactory());
            } else {
                consumer = jmsSession.createDurableConsumer(topic, subscriptionName, messageSelector, DEFAULT_NO_LOCAL);
            }
            isSuccess = true;
        } catch (JMSException e) {
            throw new ConnectorException("cannot create JMS Message Consumer", e);
        } finally {
            if (!isSuccess) {
                Utils.closeQuietly(jmsSession);
            }
        }

        return consumer;
    }
}
