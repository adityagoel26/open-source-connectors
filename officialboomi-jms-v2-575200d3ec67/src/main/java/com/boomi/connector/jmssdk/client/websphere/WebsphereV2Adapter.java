// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.GenericJndiJmsV2Adapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.IOUtil;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSContext;

import java.util.Properties;

/**
 * And adapter class to create JMS V2 components when connecting to IBM Websphere.
 */
public class WebsphereV2Adapter extends GenericJndiJmsV2Adapter {

    public WebsphereV2Adapter(AdapterSettings settings) {
        super(settings);
    }

    @Override
    protected ConnectionFactory createConnectionFactory() {
        return WebsphereComponentsFactory.createConnectionFactory(_settings);
    }

    @Override
    protected Properties createJMSProperties(AdapterSettings settings) {
        return WebsphereComponentsFactory.createJMSProperties(settings);
    }

    /**
     * Creates a {@link Destination} from the given {@code destinationName} with the {@code transactionalMode}, throwing
     * a {@link ConnectorException} if an error condition is encounter
     *
     * @param destinationName the name of the desired {@link Destination}
     * @return the {@link Destination}
     */
    @Override
    public Destination createDestination(String destinationName, int transactionalMode) {
        JMSContext context = null;
        try {
            context = newContext(transactionalMode);
            return WebsphereComponentsFactory.createDestination(destinationName, context::createQueue,
                    context::createTopic);
        } finally {
            IOUtil.closeQuietly(context);
        }
    }

    @Override
    public JMSSender createSender(int transactionalMode) {
        JMSContext context = newContext(transactionalMode);
        return new WebsphereV2Sender(context);
    }
}
