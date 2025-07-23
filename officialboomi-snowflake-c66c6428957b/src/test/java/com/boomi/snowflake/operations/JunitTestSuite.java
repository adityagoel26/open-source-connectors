// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.operations;

import com.boomi.snowflake.util.ConnectionPropertiesTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * This test suite class executes below-mentioned
 * test files in one go
 */
@RunWith(Suite.class)
@SuiteClasses({
        SnowSQLTest.class,
        SnowflakeCommandsTest.class,
        SnowflakeGetTest.class,
        SnowflakeCreateTest.class,
        SnowflakeDeleteTest.class,
        SnowflakeExecuteTest.class,
        SnowflakeUpdateTest.class,
        SnowflakeBulkLoadTest.class,
        SnowflakeBulkUnloadTest.class,
        ConnectionPropertiesTest.class,
        SnowflakeWrapperTest.class
})
public class JunitTestSuite {

}
