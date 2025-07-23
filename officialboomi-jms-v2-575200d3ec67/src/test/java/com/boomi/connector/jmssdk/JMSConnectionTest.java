// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.testutil.ConnectorTestContext;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class JMSConnectionTest {

    @Test
    public void getAdapterConfigWithPoolEnabledTest() {
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithActiveMQArtemisContext();
        context.addConnectionProperty("use_connection_pooling", true);

        JMSConnection<ConnectorTestContext> connection = new JMSConnection<>(context);
        AdapterSettings settings = connection.getAdapterSettings();

        assertThat(settings, is(notNullValue()));
        assertTrue("connection pooling should be enabled", settings.isPoolEnabled());
    }

    @Test
    public void getAdapterConfigWithPoolDisabledTest() {
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithActiveMQArtemisContext();
        context.addConnectionProperty("use_connection_pooling", false);

        JMSConnection<ConnectorTestContext> connection = new JMSConnection<>(context);
        AdapterSettings settings = connection.getAdapterSettings();

        assertThat(settings, is(notNullValue()));
        assertFalse("connection pooling should be disabled", settings.isPoolEnabled());
    }
}
