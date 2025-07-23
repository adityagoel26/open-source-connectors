// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client.websphere;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.LogUtil;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.common.CommonConstants;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.naming.Context;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Factory class with methods to construct components for JMS IBM Websphere implementation
 */
public class WebsphereComponentsFactory {

    private static final Logger LOG = LogUtil.getLogger(WebsphereComponentsFactory.class);

    /**
     * Set the transport type to TCP/IP
     */
    private static final int TRANSPORT_TYPE_CLIENT_MQ_TCP_IP = 1;

    private WebsphereComponentsFactory() {
    }

    /**
     * Create the ConnectionFactory for JMS IBM Websphere based on the configuration available in the AdapterSettings
     *
     * @param adapterSettings the adapter configuration
     * @return the connection factory
     */
    public static ConnectionFactory createConnectionFactory(AdapterSettings adapterSettings) {
        WebsphereSettings settings = adapterSettings.getWebsphereSettings();
        try {
            MQConnectionFactory factory = new MQConnectionFactory();

            if (settings.isMultiInstance()) {
                factory.setConnectionNameList(settings.getHostList());
                factory.setTransportType(CommonConstants.WMQ_CM_CLIENT);
                factory.setClientReconnectOptions(CommonConstants.WMQ_CLIENT_RECONNECT);
            } else {
                factory.setHostName(settings.getHostName());
                factory.setPort(settings.getHostPort());
                factory.setTransportType(TRANSPORT_TYPE_CLIENT_MQ_TCP_IP);
            }

            factory.setQueueManager(settings.getQueueManager());
            factory.setChannel(settings.getChannel());

            if (settings.useSsl()) {
                factory.setSSLCipherSuite(settings.getSslCipherSuite());
            }

            return factory;
        } catch (JMSException e) {
            LOG.log(Level.SEVERE, "an error happened creating the JMS Connection", e);
            throw new ConnectorException(e);
        }
    }

    /**
     * Build a Properties object containing the configuration required for creating the Initial Context
     *
     * @param adapterSettings the adapter configuration
     * @return the properties
     */
    public static Properties createJMSProperties(AdapterSettings adapterSettings) {
        Properties jmsProperties = new Properties();
        if (adapterSettings.useAuthentication()) {
            jmsProperties.setProperty(Context.SECURITY_PRINCIPAL, adapterSettings.getUsername());
            jmsProperties.setProperty(Context.SECURITY_CREDENTIALS, adapterSettings.getPassword());
        }
        return jmsProperties;
    }

    /**
     * Build a Queue or Topic reference from the provided destination name and factory methods
     *
     * @param destinationName the name of the destination
     * @param queueFactory    the factory method to create a Queue
     * @param topicFactory    the factory method to create a Topic
     * @return the destination
     */
    public static Destination createDestination(String destinationName, DestinationFactory queueFactory,
            DestinationFactory topicFactory) {
        try {
            if (destinationName.startsWith(CommonConstants.QUEUE_PREFIX)) {
                return queueFactory.create(extractDestinationName(destinationName));
            } else if (destinationName.startsWith(CommonConstants.TOPIC_PREFIX)) {
                return topicFactory.create(extractDestinationName(destinationName));
            }
            throw new ConnectorException(
                    "Invalid destination name, destination name must match " + CommonConstants.QUEUE_PREFIX + " or "
                    + CommonConstants.TOPIC_PREFIX);
        } catch (JMSException e) {
            throw new ConnectorException("cannot create destination for " + destinationName, e);
        }
    }

    /**
     * Remove the prefix from the given destination name
     *
     * @param destinationName the full destination name
     * @return the destination name without its prefix
     */
    private static String extractDestinationName(String destinationName) {
        // the "topic://" and "queue://" prefix are the same length
        return destinationName.substring(CommonConstants.TOPIC_PREFIX.length());
    }

    /**
     * Functional interface representing a factory method to create a Destination taking its name as the input parameter
     */
    @FunctionalInterface
    public interface DestinationFactory {

        /**
         * Create a {@link Destination} associated with the given name
         *
         * @param destinationName the name of the destination
         * @return the created destination
         * @throws JMSException if an error happens while creating the destination
         */
        Destination create(String destinationName) throws JMSException;
    }
}
