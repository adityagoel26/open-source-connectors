// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.JMSTestContext;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.testutil.ConnectorTestContext;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class AdapterSettingsTest {

    @Test
    public void validateCredentialsTest() {
        ConnectorTestContext context =
                new JMSTestContext.Builder().withPoolDisabled().withVersion2().withGenericService().build();
        context.addConnectionProperty("authentication", true);
        context.addConnectionProperty("username", "myUsername");
        context.addConnectionProperty("password", "myPassword");

        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings config = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));

        assertThat("AdapterSettings#useAuthentication should return true", config.useAuthentication(), is(true));
        assertThat(config.getUsername(), is("myUsername"));
        assertThat(config.getPassword(), is("myPassword"));
    }

    @Test
    public void settingsWithoutAuthenticationTest() {
        ConnectorTestContext context =
                new JMSTestContext.Builder().withVersion2().withPoolDisabled().withGenericService().build();
        context.addConnectionProperty("authentication", false);
        context.addConnectionProperty("username", "myUsername");
        context.addConnectionProperty("password", "myPassword");

        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings config = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));

        assertThat("AdapterSettings#useAuthentication should return false", config.useAuthentication(), is(false));
        assertThat(config.getUsername(), is(""));
        assertThat(config.getPassword(), is(""));
    }

    @Test
    public void settingsWithEmptyAuthenticationTest() {
        ConnectorTestContext context =
                new JMSTestContext.Builder().withVersion2().withPoolDisabled().withGenericService().build();
        context.addConnectionProperty("authentication", true);
        context.addConnectionProperty("username", null);
        context.addConnectionProperty("password", null);
        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings config = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));

        assertThat("AdapterSettings#useAuthentication should return true", config.useAuthentication(), is(true));
        assertThat(config.getUsername(), is(""));
        assertThat(config.getPassword(), is(""));
    }

    @Test
    public void getPoolConfigTest() {
        final int maxConnections = 42;
        final int minConnection = 25;
        final long maxIdleTime = 25879;
        final long maxWaitTime = 66987;
        final byte exhaustedAction = 1;

        ConnectorTestContext context = new JMSTestContext.Builder().withVersion2().withGenericService().build();
        context.addConnectionProperty("use_connection_pooling", true);
        context.addConnectionProperty("pool_maximum_connections", (long) maxConnections);
        context.addConnectionProperty("pool_minimum_connections", (long) minConnection);
        context.addConnectionProperty("pool_maximum_idle_time", maxIdleTime);
        context.addConnectionProperty("pool_maximum_wait_time", maxWaitTime);
        context.addConnectionProperty("pool_exhausted_action", "WAIT_FOR_CONNECTION");

        PropertyMap connectionProperties = context.getConnectionProperties();
        AdapterSettings config = new AdapterSettings(connectionProperties,
                new AdapterPoolSettings(connectionProperties));
        GenericObjectPool.Config poolConfig = config.getPoolConfig();

        assertThat(poolConfig.maxActive, is(maxConnections));
        assertThat(poolConfig.minIdle, is(minConnection));
        assertThat(poolConfig.softMinEvictableIdleTimeMillis, is(maxIdleTime));
        assertThat(poolConfig.maxWait, is(maxWaitTime));
        assertThat(poolConfig.whenExhaustedAction, is(exhaustedAction));
    }
}
