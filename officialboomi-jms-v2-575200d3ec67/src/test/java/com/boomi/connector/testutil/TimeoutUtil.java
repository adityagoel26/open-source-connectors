// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

public final class TimeoutUtil {

    private TimeoutUtil() {
    }

    /**
     * block the current thread for the amount of milliseconds specified
     *
     * @param millis number of milliseconds that the current thread will be blocked
     */
    public static void sleep(long millis) {
        long endTime = System.currentTimeMillis() + millis;
        while (endTime > System.currentTimeMillis()) {
            // no-op
        }
    }
}
