// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.util.StringUtil;

import org.junit.Assert;
import org.junit.Test;

/**
 * The test in this class asserts that a supported java runtime (java 8 or newer) is being used when running the
 * Integration Test Suite.
 */
public class AssertJavaVersionIT {

    @Test
    public void javaVersion8OrGreaterRequiredTest() {
        String version = System.getProperty("java.version");
        if (StringUtil.isBlank(version)) {
            Assert.fail("Cannot determine java version");
        }

        // split the version using dot as separator
        String[] parts = version.split("\\.");
        if (parts.length < 2) {
            Assert.fail("Invalid version: " + version);
        }

        String major = parts[0];

        // prior to java 9, the versions are named like 1.x ─ being x the version number
        if ("1".equals(major)) {
            // get the number after "1."
            major = parts[1];
        }

        Assert.assertTrue("Expected java 8 or newer, actual version: " + version, Integer.parseInt(major) >= 8);
    }
}
