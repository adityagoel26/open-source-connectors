// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.testutil.NoLoggingTest;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import javax.jms.JMSException;
import javax.jms.Message;

import static org.mockito.Mockito.verify;

public class MessagePropertiesDelegateTest extends NoLoggingTest {

    private static final VerificationMode ONCE = Mockito.times(1);

    @Test
    public void setCorrelationIDTest() throws JMSException {
        final String correlationID = "the correlation ID";
        Message mockedMessage = Mockito.mock(Message.class);

        MessagePropertiesDelegate sut = new MessagePropertiesDelegate(mockedMessage);

        sut.setCorrelationID(correlationID);

        verify(mockedMessage, ONCE).setJMSCorrelationID(correlationID);
    }

    @Test
    public void setCorrelationIDFailShouldNotThrowTest() throws JMSException {
        final String correlationID = "the correlation ID";
        Message mockedMessage = Mockito.mock(Message.class);
        Mockito.doThrow(JMSException.class).when(mockedMessage).setJMSCorrelationID(correlationID);

        MessagePropertiesDelegate sut = new MessagePropertiesDelegate(mockedMessage);
        sut.setCorrelationID(correlationID);
    }
}
