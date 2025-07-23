// Copyright (c) 2023 Boomi, Inc.

package com.boomi.snowflake.operations;

import com.boomi.snowflake.SnowflakeConnection;

import org.junit.Test;

import java.sql.Driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SnowflakeConnectionTest {

    private static final String CLASSNAME_SNOWFLAKE_DRIVER = "class net.snowflake.client.jdbc.SnowflakeDriver";

    /**
     * Tests to make sure that the driver is properly initialized
     */
    @Test
    public void testSnowflakeDriverLoaded() {
        Driver driver = SnowflakeConnection.getSoloDriver();
        assertNotNull(driver);
        assertEquals(CLASSNAME_SNOWFLAKE_DRIVER, driver.getClass().toString());
    }
}
