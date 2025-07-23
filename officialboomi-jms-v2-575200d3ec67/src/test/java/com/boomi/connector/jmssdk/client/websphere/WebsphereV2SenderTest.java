// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.testutil.doubles.TextMessageDouble;

import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;

import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WebsphereV2SenderTest {

    @Test
    public void setMessagePriorityInProducerTest() throws JMSException {
        JMSContext contextMock = mock(JMSContext.class, Mockito.RETURNS_DEEP_STUBS);
        JMSProducer producerMock = mock(JMSProducer.class, RETURNS_DEEP_STUBS);
        when(contextMock.createProducer()).thenReturn(producerMock);

        WebsphereV2Sender sender = new WebsphereV2Sender(contextMock);

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
