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

public class ReceiveLimitedStrategyTest {

    @Test
    public void strategyTest() {
        JMSReceiver mockedReceiver = mock(JMSReceiver.class);
        Message mockedMessage = mock(Message.class);
        when(mockedReceiver.receive()).thenReturn(mockedMessage);
        final long numberOfMessages = 2L;
        ReceiveLimitedStrategy strategy = new ReceiveLimitedStrategy(numberOfMessages);

        Message firstMessage = strategy.receiveMessage(mockedReceiver);
        boolean firstShouldContinue = strategy.shouldContinue();
        Message secondMessage = strategy.receiveMessage(mockedReceiver);
        boolean secondShouldContinue = strategy.shouldContinue();

        assertThat(mockedMessage, equalTo(firstMessage));
        assertTrue("the first call to #shouldContinue should return true as it only retrieved one message so far",
                firstShouldContinue);
        assertThat(mockedMessage, equalTo(secondMessage));
        assertFalse(
                "the second call to #shouldContinue should return false as it reached the target amount of messages",
                secondShouldContinue);
        verify(mockedReceiver, times(2)).receive();
    }
}
