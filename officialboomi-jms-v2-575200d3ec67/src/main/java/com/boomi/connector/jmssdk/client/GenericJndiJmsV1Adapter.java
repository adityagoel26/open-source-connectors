// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.util.Utils;
import com.boomi.util.LogUtil;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.naming.InitialContext;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Adapter class for JMS 1.1 Connection & Session using generic JNDI services
 * <p>
 * On instantiation, an {@link InitialContext} is created using the configuration defined in the provided
 * {@link JMSConnection}. A {@link ConnectionFactory} is obtained from that initial context, which is used to create a
 * {@link Connection}. Finally, a {@link Session} is created from the {@link Connection}.
 * <p>
 * Both {@link Connection} & {@link Session} are held by this class to be properly closed when invoking
 * {@link GenericJndiJmsV1Adapter#close()}. At that point, the {@link InitialContext} is also closed by a super call to
 * {@link GenericJndiBaseAdapter#close()}
 */
public class GenericJndiJmsV1Adapter extends GenericJndiBaseAdapter {

    private static final Logger LOG = LogUtil.getLogger(GenericJndiJmsV1Adapter.class);

    private final Connection _jmsConnection;

    public GenericJndiJmsV1Adapter(AdapterSettings settings) {
        super(settings);
        _jmsConnection = initConnection();
    }

    private Connection initConnection() {
        final Connection jmsConnection;
        boolean isSuccess = false;
        try {
            jmsConnection = createJMSConnection();
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                super.close();
            }
        }
        return jmsConnection;
    }

    protected Session createSession(int transactionalMode) {
        boolean useTransactions = Session.SESSION_TRANSACTED == transactionalMode;
        try {
            return _jmsConnection.createSession(useTransactions, transactionalMode);
        } catch (JMSException e) {
            LOG.log(Level.SEVERE, "an error happened creating the JMS Session", e);
            throw new ConnectorException(e);
        }
    }

    @Override
    public JMSSender createSender(int transactionalMode) {
        Session session = createSession(transactionalMode);
        return new JMSV1Sender(session);
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSV1Receiver(session, destination, messageSelector);
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSV1Receiver(session, destination, subscriptionName, messageSelector);
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSV1Listener(session, destination, messageSelector);
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        Session session = createSession(transactionalMode);
        return new JMSV1Listener(session, destination, subscriptionName, messageSelector);
    }

    @Override
    public boolean validate() {
        // in order to validate if the connection is valid, a session is created and destroyed
        Session session = null;
        try {
            session = createSession(Session.CLIENT_ACKNOWLEDGE);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "error while validating JMS Adapter", e);
            return false;
        } finally {
            Utils.closeQuietly(session);
        }

        return true;
    }

    @Override
    public void deactivate() {
        try {
            _jmsConnection.setExceptionListener(null);
            _jmsConnection.stop();
        } catch (JMSException e) {
            LOG.log(Level.WARNING, "error while stopping JMS connection", e);
        }
    }

    protected Connection createJMSConnection() {
        ConnectionFactory connectionFactory = createConnectionFactory();
        Connection connection;
        try {
            if (_settings.useAuthentication()) {
                connection = connectionFactory.createConnection(_settings.getUsername(), _settings.getPassword());
            } else {
                connection = connectionFactory.createConnection();
            }
            connection.start();
        } catch (JMSException e) {
            LOG.log(Level.SEVERE, "an error happened creating the JMS Connection", e);
            throw new ConnectorException(e);
        }
        return connection;
    }


    @Override
    public void start() {
        try {
            _jmsConnection.start();
        } catch (JMSException e) {
            throw new ConnectorException("cannot start Starts a connection's delivery of incoming messages.", e);
        }
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) {
        try {
            _jmsConnection.setExceptionListener(exceptionListener);
        } catch (JMSException e) {
            throw new ConnectorException("cannot set exception listener", e);
        }
    }

    /**
     * Close the {@link javax.naming.Context}, {@link Connection} & {@link Session} held by this instance
     */
    @Override
    public void close() {
        Utils.closeQuietly(_jmsConnection);
        super.close();
    }
}
