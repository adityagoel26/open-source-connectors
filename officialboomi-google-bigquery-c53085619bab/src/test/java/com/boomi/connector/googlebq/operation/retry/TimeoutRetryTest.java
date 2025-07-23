// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.retry;

import org.junit.Test;

import static org.junit.Assert.assertFalse;

/**
 * @author Rohan Jain
 */
public class TimeoutRetryTest {

    @Test
    public void testRetryWithZeroTimeout() {
        TimeoutRetry retry = new TimeoutRetry(0L);
        boolean shouldRetry = retry.shouldRetryImpl(5, null);
        assertFalse("This should not retry as timeout is 0", shouldRetry);
    }

    @Test
    public void testRetryWithPositiveTimeout() throws InterruptedException {
        TimeoutRetry retry = new TimeoutRetry(1L);
        //allows elapsed time to go past timeout value of 1 milli second
        Thread.sleep(2); //NOSONAR
        boolean shouldRetry = retry.shouldRetryImpl(5, null);
        assertFalse("Should not retry as timeout is 1 milli second which has been passed", shouldRetry);
    }

    @Test
    public void testRetryWithNegativeTimeout() {
        TimeoutRetry retry = new TimeoutRetry(-32L);
        boolean shouldRetry = retry.shouldRetryImpl(5, null);
        assertFalse("Should not retry as timeout is negative", shouldRetry);
    }
}
