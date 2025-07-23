// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk;

import com.boomi.connector.api.Connector;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.testutil.ConnectorTestContext;

public class JMSTestContext extends ConnectorTestContext {

    /**
     * Provide a Context with the credentials needed to authenticate against an Active MQ Artemis Server
     */
    public static JMSTestContext getJMS2GenericWithActiveMQArtemisContext() {
        final String username = "<the ActiveMQ Artemis username>";
        final String password = "<the ActiveMQ Artemis password>";
        final String serverUrl = "<the ActiveMQ Artemis server url>";

        JMSTestContext context = new Builder().withGenericService().withVersion2().withPoolDisabled().build();

        context.addConnectionProperty("authentication", true);
        context.addConnectionProperty("username", username);
        context.addConnectionProperty("password", password);
        context.addConnectionProperty("jndi_lookup_factory", "ConnectionFactory");
        context.addConnectionProperty("initial_context_factory",
                "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
        context.addConnectionProperty("provider_url", serverUrl);

        return context;
    }

    /**
     * Provide a Context with the credentials needed to authenticate against Azure Service Bus
     */
    public static JMSTestContext getJMS2GenericWithAzureServiceBusContext(String serverUrl) {
        final String username = "<the Azure Service Bus username>";
        final String password = "<the Azure Service Bus password>";

        JMSTestContext context = new Builder().withGenericService().withPoolDisabled().withVersion2().build();

        context.addConnectionProperty("authentication", true);
        context.addConnectionProperty("username", username);
        context.addConnectionProperty("password", password);
        context.addConnectionProperty("jndi_lookup_factory", "SBCF");
        context.addConnectionProperty("initial_context_factory", "org.apache.qpid.jms.jndi.JmsInitialContextFactory");
        context.addConnectionProperty("provider_url", serverUrl);

        return context;
    }

    /**
     * Provide a Context with the credentials needed to authenticate against ActiveMQ Classic
     */
    public static JMSTestContext getJMS1GenericWithActiveMQClassicContext() {
        final String serverUrl = "<the ActiveMQ Classic server url>";

        JMSTestContext context = new Builder().withGenericService().withPoolDisabled().withVersion1().build();

        context.addConnectionProperty("authentication", false);
        context.addConnectionProperty("jndi_lookup_factory", "ConnectionFactory");
        context.addConnectionProperty("initial_context_factory",
                "org.apache.activemq.jndi.ActiveMQInitialContextFactory");
        context.addConnectionProperty("provider_url", serverUrl);

        return context;
    }

    @Override
    protected Class<? extends Connector> getConnectorClass() {
        return JMSConnector.class;
    }

    public static class Builder {

        private final JMSTestContext _context;

        public Builder() {
            _context = new JMSTestContext();
        }

        public Builder withVersion2() {
            _context.addConnectionProperty("version", "V2_0");
            return this;
        }

        public Builder withVersion1() {
            _context.addConnectionProperty("version", "V1_1");
            return this;
        }

        public Builder withGenericService() {
            _context.addConnectionProperty("server_type", "GENERIC_JNDI");
            return this;
        }

        public Builder withSendOperation() {
            _context.setOperationType(OperationType.CREATE);
            _context.setOperationCustomType("SEND");
            return this;
        }

        public Builder withPoolDisabled() {
            _context.addConnectionProperty("use_connection_pooling", false);
            _context.addConnectionProperty("pool_maximum_connections", 10L);
            _context.addConnectionProperty("pool_minimum_connections", 0L);
            _context.addConnectionProperty("pool_maximum_idle_time", 10_000L);
            _context.addConnectionProperty("pool_maximum_wait_time", 10_000L);
            _context.addConnectionProperty("pool_exhausted_action", "WAIT_FOR_CONNECTION");
            return this;
        }

        public Builder withPoolEnabled() {
            _context.addConnectionProperty("use_connection_pooling", true);
            _context.addConnectionProperty("pool_maximum_connections", 10L);
            _context.addConnectionProperty("pool_minimum_connections", 0L);
            _context.addConnectionProperty("pool_maximum_idle_time", 10_000L);
            _context.addConnectionProperty("pool_maximum_wait_time", 10_000L);
            _context.addConnectionProperty("pool_exhausted_action", "WAIT_FOR_CONNECTION");
            return this;
        }

        public JMSTestContext build() {
            return _context;
        }
    }
}
