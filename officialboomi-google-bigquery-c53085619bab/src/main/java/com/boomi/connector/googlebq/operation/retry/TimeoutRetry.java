// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.retry;

import com.boomi.util.retry.PhasedRetry;


/**
 * Specialized Phased Retry which determines if it should retry based on a timeout
 * rather than number of attempts. If timeout is configured as -1 then retry
 * is performed indefinitely
 *
 * @author Rohan Jain
 */
public class TimeoutRetry extends PhasedRetry {

    private static final int NANO_TO_MILLIS_DIVISOR = 1000000;
    private static final int[] JOB_DELAYS = new int[] { 0, 1, 2, 5, 10, 15, 20 };

    private final long _timeout;
    private long _startTime;

    /**
     * Initializes a new {@link TimeoutRetry} instance
     *
     * @param timeout
     *         : timeout in milliseconds
     */
    public TimeoutRetry(long timeout) {
        super(Integer.MAX_VALUE, JOB_DELAYS);
        _timeout = timeout;
        resetTimer();
    }

    public final void resetTimer() {
        _startTime = System.nanoTime();
    }

    /**
     * Returns true if the elapsed time since {@link TimeoutRetry} was initialized
     * or {@link TimeoutRetry#resetTimer()} was called has exceeded the timeout.
     *
     * Returns true if the timeout configured is -1. This should be used to retry indefinitely
     * @param retryNumber
     * @param status
     * @return
     */
    @Override
    protected boolean shouldRetryImpl(int retryNumber, Object status) {
        if(_timeout == -1) {
            return true;
        }

        //need to convert nanoseconds in milliseconds
        long elapsedTime = (System.nanoTime() - _startTime) / NANO_TO_MILLIS_DIVISOR;
        return elapsedTime < _timeout;
    }
}
