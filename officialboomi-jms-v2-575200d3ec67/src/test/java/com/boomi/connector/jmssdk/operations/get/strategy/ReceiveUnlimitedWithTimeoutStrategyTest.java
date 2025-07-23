// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.testutil.TimeoutUtil;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReceiveUnlimitedWithTimeoutStrategyTest {

    @Test
    public void testTimeoutStrategy() {
        final long timeout = 10L;
        ReceiveUnlimitedWithTimeoutStrategy strategy = new ReceiveUnlimitedWithTimeoutStrategy(timeout);

        boolean firstShouldContinue = strategy.shouldContinue();
        TimeoutUtil.sleep(20L);
        boolean secondShouldContinue = strategy.shouldContinue();

        assertTrue("the first call to #shouldContinue should return true as it was executed within the timeout frame",
                firstShouldContinue);
        assertFalse("the second call to #shouldContinue should return false as was executed after the timeout expired",
                secondShouldContinue);
    }
}
