// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectionTester;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.testutil.ConnectorTestContext;
import com.boomi.connector.testutil.NoLoggingTest;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Integration tests that verify the «Test connection» behaviour against different services
 */
@Ignore("Configure JMSTestContext with valid credentials to run these integration tests")
public class ConnectionTesterIT extends NoLoggingTest {

    @Test
    public void activeMQShouldPassTest() {
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithActiveMQArtemisContext();

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<>(context));
        connectionTester.testConnection();
    }

    @Test(expected = ConnectorException.class)
    public void activeMQShouldFailTest() {
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithActiveMQArtemisContext();
        // replace the username for a wrong one
        context.addConnectionProperty("username", "invalid");

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<BrowseContext>(context));
        connectionTester.testConnection();
    }

    @Test
    public void azureServiceBusShouldPassTest() {
        String providerUrl = this.getClass().getResource("/azure-service-bus.properties").toString();
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithAzureServiceBusContext(providerUrl);

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<BrowseContext>(context));
        connectionTester.testConnection();
    }

    @Test(expected = ConnectorException.class)
    public void azureServiceBusShouldFailTest() {
        String providerUrl = this.getClass().getResource("/azure-service-bus-invalid.properties").toString();
        ConnectorTestContext context = JMSTestContext.getJMS2GenericWithAzureServiceBusContext(providerUrl);

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<BrowseContext>(context));
        connectionTester.testConnection();
    }

    @Test
    public void activeMQClassicShouldPassTest() {
        ConnectorTestContext context = JMSTestContext.getJMS1GenericWithActiveMQClassicContext();

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<BrowseContext>(context));
        connectionTester.testConnection();
    }

    @Test(expected = ConnectorException.class)
    public void activeMQClassicShouldFailTest() {
        ConnectorTestContext context = JMSTestContext.getJMS1GenericWithActiveMQClassicContext();
        // replace url for a wrong one
        context.addConnectionProperty("provider_url", "tcp://labqawin1.lab.boomi.com:6000");

        ConnectionTester connectionTester = new JMSBrowser(new JMSConnection<BrowseContext>(context));
        connectionTester.testConnection();
    }
}
