// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil;

import com.boomi.util.TestUtil;

import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Extend this class to avoid logging errors when executing tests
 */
public abstract class NoLoggingTest {

    @BeforeClass
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @AfterClass
    public static void enableLogs() {
        TestUtil.restoreLog();
    }
}
