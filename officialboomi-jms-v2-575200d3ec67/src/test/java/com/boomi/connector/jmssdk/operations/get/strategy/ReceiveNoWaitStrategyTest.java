// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;

import org.junit.Test;

import javax.jms.Message;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReceiveNoWaitStrategyTest {

    @Test
    public void strategyTest() {
        JMSReceiver mockedReceiver = mock(JMSReceiver.class);
        Message mockedMessage = mock(Message.class);
        when(mockedReceiver.receiveNoWait()).thenReturn(mockedMessage);
        ReceiveNoWaitStrategy strategy = new ReceiveNoWaitStrategy();

        Message receive = strategy.receiveMessage(mockedReceiver);
        boolean shouldContinue = strategy.shouldContinue();

        assertThat(mockedMessage, equalTo(receive));
        assertFalse("the strategy should return false after getting a message", shouldContinue);
        verify(mockedReceiver, times(1)).receiveNoWait();
    }

    @Test
    public void shouldContinueInvokedOnceTest() {
        ReceiveNoWaitStrategy strategy = new ReceiveNoWaitStrategy();

        boolean shouldContinue = strategy.shouldContinue();

        assertTrue("the strategy should return true for before any invocation of #receive", shouldContinue);
    }
}
