// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.GenericJndiJmsV1Adapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.util.Utils;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Session;

import java.util.Properties;

/**
 * And adapter class to create JMS V1.1 components when connecting to IBM Websphere.
 */
public class WebsphereV1Adapter extends GenericJndiJmsV1Adapter {

    public WebsphereV1Adapter(AdapterSettings settings) {
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
        Session session = null;
        try {
            session = createSession(transactionalMode);
            return WebsphereComponentsFactory.createDestination(destinationName, session::createQueue,
                    session::createTopic);
        } finally {
            Utils.closeQuietly(session);
        }
    }

    @Override
    public JMSSender createSender(int transactionalMode) {
        Session session = createSession(transactionalMode);
        return new WebsphereV1Sender(session);
    }
}
