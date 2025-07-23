// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import oracle.jms.AdtMessage;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.jmssdk.operations.model.GenericTargetDestination;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.LogUtil;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class provides a common abstraction for connecting to a JMS Generic JNDI Service regardless of the underling
 * service or JMS Version.
 * <p>
 * The {@link Context} is held by this class to be properly closed when invoking
 * {@link GenericJndiBaseAdapter#close()}.
 */
public abstract class GenericJndiBaseAdapter implements Closeable {

    private static final Logger LOG = LogUtil.getLogger(GenericJndiBaseAdapter.class);

    private final Context _initialContext;
    protected final AdapterSettings _settings;

    GenericJndiBaseAdapter(AdapterSettings settings) {
        _settings = settings;
        _initialContext = createInitialContext();
    }

    protected Properties createJMSProperties(AdapterSettings settings) {
        Properties jmsProperties = new Properties();

        if (settings.useAuthentication()) {
            jmsProperties.setProperty(Context.SECURITY_PRINCIPAL, settings.getUsername());
            jmsProperties.setProperty(Context.SECURITY_CREDENTIALS, settings.getPassword());
        }

        String initialContextFactory = settings.getInitialContextFactory();
        jmsProperties.setProperty(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);

        String providerUrl = settings.getProviderUrl();
        jmsProperties.setProperty(Context.PROVIDER_URL, providerUrl);

        for (Map.Entry<String, String> customProperty : Utils.nullSafe(settings.getJmsProperties()).entrySet()) {
            jmsProperties.setProperty(customProperty.getKey(), customProperty.getValue());
        }
        return jmsProperties;
    }

    protected final Context createInitialContext() {
        Properties configs = createJMSProperties(_settings);
        try {
            return new InitialContext(configs);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "error creating initial context", e);
            throw new ConnectorException(e);
        }
    }

    /**
     * Creates a {@link Destination} from the given {@code destinationName} with the {@code transactionalMode}, throwing
     * a {@link ConnectorException} if an error condition is encounter
     *
     * @param destinationName the name of the desired {@link Destination}
     * @return the {@link Destination}
     */
    public Destination createDestination(String destinationName, int transactionalMode) {
        try {
            return (Destination) _initialContext.lookup(destinationName);
        } catch (NamingException e) {
            throw new ConnectorException(
                    String.format("cannot create destination for %s - transactionalMode %s", destinationName,
                            transactionalMode), e);
        }
    }

    /**
     * Get the {@link DestinationType} associated with the given {@link Message}. This method throws a
     * {@link ConnectorException} if the message type is not supported
     *
     * @param message to extract the {@link DestinationType}
     * @return the {@link DestinationType}
     */
    public DestinationType getDestinationType(Message message) {
        if (message instanceof BytesMessage) {
            return DestinationType.BYTE_MESSAGE;
        }

        if (message instanceof TextMessage) {

            return DestinationType.TEXT_MESSAGE;
        }

        if (message instanceof MapMessage) {
            return DestinationType.MAP_MESSAGE;
        }

        if (message instanceof AdtMessage) {
            return DestinationType.ADT_MESSAGE;
        }

        throw new ConnectorException("unknown destination type for: " + message.getClass());
    }

    /**
     * Creates a concrete {@link JMSSender} for the JMS Version of the Adapter
     *
     * @param transactionalMode indicate whether the sender will produce the messages within a transaction or not.
     * @return the {@link JMSSender}
     */
    public abstract JMSSender createSender(int transactionalMode);

    /**
     * Creates a concrete {@link JMSReceiver} for the JMS Version of the Adapter
     *
     * @param targetDestination the destination from where messages will be received
     * @param messageSelector   only messages with properties matching the message selector expression are delivered. A
     *                          value of null or an empty string indicates that there is no message selector for the
     *                          message consumer.
     * @param transactionalMode indicate whether the receiver will get the messages within a transaction or not.
     * @return the {@link JMSReceiver}
     */
    public abstract JMSReceiver createReceiver(TargetDestination targetDestination, String messageSelector,
            int transactionalMode);

    /**
     * Creates a concrete {@link JMSReceiver} for the JMS Version of the Adapter
     *
     * @param targetDestination the topic from where messages will be received
     * @param subscriptionName  the subscription used to receive the messages
     * @param messageSelector   only messages with properties matching the message selector expression are delivered. A
     *                          value of null or an empty string indicates that there is no message selector for the
     *                          message consumer.
     * @param transactionalMode indicate whether the receiver will get the messages within a transaction or not.
     * @return the {@link JMSReceiver}
     */
    public abstract JMSReceiver createReceiver(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode);

    /**
     * Creates a concrete {@link JMSListener} for the JMS Version of the Adapter
     *
     * @param targetDestination the destination that the listener will listen.
     * @param messageSelector   only messages with properties matching the message selector expression are delivered. A
     *                          value of null or an empty string indicates that there is no message selector for the
     *                          message consumer.
     * @param transactionalMode indicate whether the receiver will get the messages within a transaction or not.
     * @return @{@link JMSListener}
     */
    public abstract JMSListener createListener(TargetDestination targetDestination, String messageSelector,
            int transactionalMode);

    /**
     * Creates a concrete {@link JMSListener} for the JMS Version of the Adapter
     *
     * @param targetDestination the destination that the listener will listen.
     * @param subscriptionName  the subscription used to listen the messages
     * @param messageSelector   only messages with properties matching the message selector expression are delivered. A
     *                          value of null or an empty string indicates that there is no message selector for the
     *                          message consumer.
     * @param transactionalMode indicate whether the receiver will get the messages within a transaction or not.
     * @return @{@link JMSListener}
     */
    public abstract JMSListener createListener(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode);

    public abstract void setExceptionListener(ExceptionListener exceptionListener);

    public abstract boolean validate();

    public abstract void deactivate();

    /**
     * Create a TargetDestination from of the id
     *
     * @param id
     * @return @{@link TargetDestination}
     */
    public TargetDestination createTargetDestination(String id) {
        return new GenericTargetDestination(id);
    }

    /**
     * Create a TargetDestination from of destinationName and destinationType
     *
     * @param destinationName identification of destination target
     * @param destinationType the destination type could be queue or topic
     * @return @{@link TargetDestination}
     */
    public TargetDestination createTargetDestination(String destinationName, DestinationType destinationType) {
        TargetDestination targetDestination = new GenericTargetDestination(destinationName);
        targetDestination.setDestinationType(destinationType);
        return targetDestination;
    }

    /**
     * Creates a targetDestination and loads from the service its data, if the method is not redefined its behavior will
     * be as the createTargetDestination method.
     *
     * @param destinationName
     * @return @{@link TargetDestination}
     */
    public TargetDestination createAndLoadTargetDestination(String destinationName) {
        return createTargetDestination(destinationName, null);
    }

    /**
     * return the adapter settings
     *
     * @return @{@link AdapterSettings}
     */
    public AdapterSettings getSettings() {
        return _settings;
    }

    /**
     * return the object definitions according to the targetDestination
     *
     * @param targetDestination
     * @return @{@link ObjectDefinition}
     */
    public ObjectDefinition getObjectDefinition(TargetDestination targetDestination) {
        return targetDestination.getObjectDefinition();
    }

    protected ConnectionFactory createConnectionFactory() {
        try {
            String connectionFactoryLookup = _settings.getJndiLookupFactory();
            return (ConnectionFactory) _initialContext.lookup(connectionFactoryLookup);
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "error creating connection factory", e);
            throw new ConnectorException(e);
        }
    }

    /**
     * Starts (or restarts) a connection's delivery of incoming messages.
     */
    public abstract void start();

    /**
     * Close the {@link javax.naming.Context} held by this instance
     */
    @Override
    public void close() {
        Utils.closeQuietly(_initialContext);
    }
}
