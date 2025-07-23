// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.JMSReceiver;
import com.boomi.connector.testutil.TimeoutUtil;

import org.junit.Test;

import javax.jms.Message;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReceiveLimitedWithTimeoutStrategyTest {

    @Test
    public void testMessageLimitStrategy() {
        JMSReceiver mockedReceiver = mock(JMSReceiver.class);
        Message mockedMessage = mock(Message.class);
        when(mockedReceiver.receive(anyLong())).thenReturn(mockedMessage);
        final long messages = 2L;
        final long unlimitedTimeout = Integer.MAX_VALUE;
        ReceiveLimitedWithTimeoutStrategy strategy = new ReceiveLimitedWithTimeoutStrategy(unlimitedTimeout, messages);

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
        verify(mockedReceiver, times(2)).receive(anyLong());
    }

    @Test
    public void testTimeoutStrategy() {
        final long unlimitedMessages = Integer.MAX_VALUE;
        final long timeout = 10L;
        ReceiveLimitedWithTimeoutStrategy strategy = new ReceiveLimitedWithTimeoutStrategy(timeout, unlimitedMessages);

        boolean firstShouldContinue = strategy.shouldContinue();
        TimeoutUtil.sleep(20L);
        boolean secondShouldContinue = strategy.shouldContinue();

        assertTrue("the first call to #shouldContinue should return true as it was executed within the timeout frame",
                firstShouldContinue);
        assertFalse("the second call to #shouldContinue should return false as was executed after the timeout expired",
                secondShouldContinue);
    }

}
