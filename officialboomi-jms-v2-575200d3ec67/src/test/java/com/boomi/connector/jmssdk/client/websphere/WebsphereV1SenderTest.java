// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.testutil.doubles.TextMessageDouble;

import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebsphereV1SenderTest {

    @Test
    public void setMessagePriorityInProducerTest() throws JMSException {
        Session sessionMock = mock(Session.class, Mockito.RETURNS_DEEP_STUBS);
        MessageProducer producerMock = mock(MessageProducer.class, RETURNS_DEEP_STUBS);
        when(sessionMock.createProducer(any())).thenReturn(producerMock);

        WebsphereV1Sender sender = new WebsphereV1Sender(sessionMock);

        Destination destination = new Destination() {
        };
        Message message = new TextMessageDouble();
        long timeToLive = 42L;

        int expectedPriority = 2;
        message.setJMSPriority(expectedPriority);

        sender.send(destination, message, timeToLive);

        verify(producerMock, times(1)).setPriority(expectedPriority);
        verify(producerMock, times(1)).setTimeToLive(timeToLive);
        verify(producerMock, times(1)).send(destination, message);
    }
}
