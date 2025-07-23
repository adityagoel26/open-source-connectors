// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.IOUtil;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import java.io.Closeable;
import java.util.Properties;

public final class ClientTestUtils {

    private ClientTestUtils() {
    }

    public static <T extends Message> T getLastMessage(JMSConnection<OperationContext> connection,
            String destinationName, Class<T> expectedMessageType) {
        JMS2Holder<JMSConsumer> consumer = createJMS2Consumer(connection, destinationName);
        Message message = null;
        do {
            Message received = consumer._entity.receive(500L);
            if (received == null) {
                consumer.close();
                return expectedMessageType.cast(message);
            }
            message = received;
        } while (true);
    }

    public static Message publish(JMSOperationConnection connection, String payload, String destinationName)
            throws JMSException {
        JMS2Holder<JMSProducer> jms2Producer = null;
        TextMessage message;
        try {
            jms2Producer = createJMS2Producer(connection);
            Destination destination = getDestination(destinationName, jms2Producer._initialContext);
            message = jms2Producer._context.createTextMessage();
            message.setText(payload);
            jms2Producer._entity.send(destination, message);
        } finally {
            IOUtil.closeQuietly(jms2Producer);
        }

        return message;
    }

    private static JMS2Holder<JMSProducer> createJMS2Producer(JMSOperationConnection connection) {
        AdapterSettings settings = connection.getAdapterSettings();
        InitialContext initialContext = createInitialContext(settings);
        JMSContext jmsContext = createJMS2Context(settings, initialContext);

        return new JMS2Holder<>(initialContext, jmsContext, jmsContext.createProducer());
    }

    private static JMS2Holder<JMSConsumer> createJMS2Consumer(JMSConnection<OperationContext> connection,
            String destinationName) {
        AdapterSettings settings = connection.getAdapterSettings();
        InitialContext initialContext = createInitialContext(settings);
        JMSContext jmsContext = createJMS2Context(settings, initialContext);

        Destination destination = getDestination(destinationName, initialContext);

        return new JMS2Holder<>(initialContext, jmsContext, jmsContext.createConsumer(destination));
    }

    private static Destination getDestination(String destinationName, InitialContext initialContext) {
        Destination destination;
        try {
            destination = (Destination) initialContext.lookup(destinationName);
        } catch (NamingException e) {
            throw new ConnectorException("cannot create destination for " + destinationName, e);
        }
        return destination;
    }

    private static InitialContext createInitialContext(AdapterSettings settings) {
        Properties jmsProperties = new Properties();

        if (settings.useAuthentication()) {
            jmsProperties.setProperty(Context.SECURITY_PRINCIPAL, settings.getUsername());
            jmsProperties.setProperty(Context.SECURITY_CREDENTIALS, settings.getPassword());
        }

        String initialContextFactory = settings.getInitialContextFactory();
        jmsProperties.setProperty(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);

        String providerUrl = settings.getProviderUrl();
        jmsProperties.setProperty(Context.PROVIDER_URL, providerUrl);

        try {
            return new InitialContext(jmsProperties);
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    private static JMSContext createJMS2Context(AdapterSettings settings, InitialContext context) {
        ConnectionFactory connectionFactory;
        try {
            String connectionFactoryLookup = settings.getJndiLookupFactory();
            connectionFactory = (ConnectionFactory) context.lookup(connectionFactoryLookup);
        } catch (Exception e) {
            throw new ConnectorException(e);
        }

        if (settings.useAuthentication()) {
            return connectionFactory.createContext(settings.getUsername(), settings.getPassword(),
                    JMSContext.AUTO_ACKNOWLEDGE);
        } else {
            return connectionFactory.createContext(JMSContext.AUTO_ACKNOWLEDGE);
        }
    }

    public static final class JMS2Holder<T> implements Closeable {

        private final T _entity;
        private final JMSContext _context;
        private final InitialContext _initialContext;

        private JMS2Holder(InitialContext initialContext, JMSContext context, T entity) {
            _initialContext = initialContext;
            _context = context;
            _entity = entity;
        }

        @Override
        public void close() {
            if (_entity instanceof AutoCloseable) {
                IOUtil.closeQuietly((AutoCloseable) _entity);
            }

            IOUtil.closeQuietly(_context);
            try {
                _initialContext.close();
            } catch (NamingException e) {
                // no-op
            }
        }
    }
}
