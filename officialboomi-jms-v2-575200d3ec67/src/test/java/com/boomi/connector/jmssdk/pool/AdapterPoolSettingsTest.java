// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.testutil.MutablePropertyMap;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;

import static java.lang.Math.abs;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class AdapterPoolSettingsTest {

    @Test
    public void equalsAndHashCodePositiveTest() {
        MutablePropertyMap properties = new MutablePropertyMap();
        properties.put("use_connection_pooling", true);
        properties.put("pool_maximum_connections", 10L);
        properties.put("pool_minimum_connections", 0L);
        properties.put("pool_maximum_idle_time", 10_000L);
        properties.put("pool_maximum_wait_time", 10_000L);
        properties.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

        AdapterPoolSettings configs1 = new AdapterPoolSettings(properties);
        AdapterPoolSettings configs2 = new AdapterPoolSettings(properties);

        assertThat(configs1, is(configs2));
        assertThat(configs1.hashCode(), is(configs2.hashCode()));
    }

    @Test
    public void validateValuesTest() {
        MutablePropertyMap properties = new MutablePropertyMap();
        Random r = new Random(System.currentTimeMillis());
        long maxConnections = abs(r.nextInt());
        long minConnections = abs(r.nextInt());
        long maxIdleTime = abs(r.nextInt());
        long maxWaitTime = abs(r.nextInt());
        String exhaustedAction = "IMMEDIATELY_FAIL";

        properties.put("use_connection_pooling", true);
        properties.put("pool_maximum_connections", maxConnections);
        properties.put("pool_minimum_connections", minConnections);
        properties.put("pool_maximum_idle_time", maxIdleTime);
        properties.put("pool_maximum_wait_time", maxWaitTime);
        properties.put("pool_exhausted_action", exhaustedAction);

        AdapterPoolSettings config = new AdapterPoolSettings(properties);
        GenericObjectPool.Config actualConfig = config.toConfig();

        assertThat(actualConfig.maxActive, is((int) maxConnections));
        assertThat(actualConfig.minIdle, is((int) minConnections));
        assertThat(actualConfig.softMinEvictableIdleTimeMillis, is(maxIdleTime));
        assertThat(actualConfig.maxWait, is(maxWaitTime));
        assertThat(actualConfig.whenExhaustedAction, is(GenericObjectPool.WHEN_EXHAUSTED_FAIL));
    }

    @Test
    public void disabledPoolTest() {
        MutablePropertyMap properties = new MutablePropertyMap();
        properties.put("use_connection_pooling", false);

        AdapterPoolSettings config = new AdapterPoolSettings(properties);
        assertThat(config.isEnabled(), is(false));
    }

    @RunWith(Parameterized.class)
    public static class EqualsAndHashCodeNegativeTest {

        @Parameterized.Parameters(name = "{2}")
        public static Collection<Object[]> adapterPoolConfigs() {
            Collection<Object[]> testCases = new ArrayList<>();

            {
                String description = "different use_connection_pooling";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", false);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 0L);
                props1.put("pool_maximum_idle_time", 10_000L);
                props1.put("pool_maximum_wait_time", 10_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 10L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            {
                String description = "different pool_maximum_connections";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", true);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 0L);
                props1.put("pool_maximum_idle_time", 10_000L);
                props1.put("pool_maximum_wait_time", 10_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 11L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            {
                String description = "different pool_minimum_connections";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", true);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 42L);
                props1.put("pool_maximum_idle_time", 10_000L);
                props1.put("pool_maximum_wait_time", 10_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 10L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            {
                String description = "different pool_maximum_idle_time";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", true);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 0L);
                props1.put("pool_maximum_idle_time", 60_000L);
                props1.put("pool_maximum_wait_time", 10_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 10L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            {
                String description = "different pool_maximum_wait_time";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", true);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 0L);
                props1.put("pool_maximum_idle_time", 10_000L);
                props1.put("pool_maximum_wait_time", 445_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 10L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            {
                String description = "different pool_exhausted_action";

                MutablePropertyMap props1 = new MutablePropertyMap();
                props1.put("use_connection_pooling", true);
                props1.put("pool_maximum_connections", 10L);
                props1.put("pool_minimum_connections", 0L);
                props1.put("pool_maximum_idle_time", 10_000L);
                props1.put("pool_maximum_wait_time", 10_000L);
                props1.put("pool_exhausted_action", "WAIT_FOR_CONNECTION");

                MutablePropertyMap props2 = new MutablePropertyMap();
                props2.put("use_connection_pooling", true);
                props2.put("pool_maximum_connections", 10L);
                props2.put("pool_minimum_connections", 0L);
                props2.put("pool_maximum_idle_time", 10_000L);
                props2.put("pool_maximum_wait_time", 10_000L);
                props2.put("pool_exhausted_action", "IMMEDIATELY_FAIL");

                testCases.add(new Object[] {
                        new AdapterPoolSettings(props1), new AdapterPoolSettings(props2), description });
            }

            return testCases;
        }

        private final AdapterPoolSettings _config1;
        private final AdapterPoolSettings _config2;

        public EqualsAndHashCodeNegativeTest(AdapterPoolSettings config1, AdapterPoolSettings config2,
                String description) {
            _config1 = config1;
            _config2 = config2;
        }

        @Test
        public void equalsAndHashCodeNegativeTest() {
            assertThat(_config1, is(not(_config2)));
            assertThat(_config1.hashCode(), is(not(_config2.hashCode())));
        }
    }
}
