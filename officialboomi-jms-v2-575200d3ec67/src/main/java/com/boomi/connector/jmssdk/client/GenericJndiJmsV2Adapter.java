// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.client;

import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSContext;
import javax.naming.InitialContext;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Adapter class for JMS 2.0 Context using generic JNDI services.
 * <p>
 * On instantiation, an {@link InitialContext} is created using the configuration defined in the provided
 * {@link JMSConnection}. A {@link ConnectionFactory} is obtained from that initial context, which is finally used to
 * create a {@link JMSContext}.
 * <p>
 * The {@link JMSContext} is held by this class to be properly closed when invoking
 * {@link GenericJndiJmsV2Adapter#close()}. At that point, the {@link InitialContext} is also closed by a super call to
 * {@link GenericJndiBaseAdapter#close()}
 */
public class GenericJndiJmsV2Adapter extends GenericJndiBaseAdapter {

    private static final Logger LOG = LogUtil.getLogger(GenericJndiJmsV2Adapter.class);

    private final JMSContext _jmsContext;

    public GenericJndiJmsV2Adapter(AdapterSettings settings) {
        super(settings);
        _jmsContext = initJmsContext();
    }

    private JMSContext initJmsContext() {
        final JMSContext jmsContext;
        boolean isSuccess = false;
        try {
            jmsContext = createJMSContext();
            isSuccess = true;
        } finally {
            if (!isSuccess) {
                super.close();
            }
        }
        return jmsContext;
    }

    @Override
    public JMSSender createSender(int transactionalMode) {
        JMSContext context = newContext(transactionalMode);
        return new JMSV2Sender(context);
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        JMSContext context = newContext(transactionalMode);
        return new JMSV2Receiver(context, destination, messageSelector);
    }

    @Override
    public JMSReceiver createReceiver(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        JMSContext context = newContext(transactionalMode);
        return new JMSV2Receiver(context, destination, subscriptionName, messageSelector);
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String messageSelector,
            int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        JMSContext context = newContext(transactionalMode);
        return new JMSV2Listener(context, destination, messageSelector);
    }

    @Override
    public JMSListener createListener(TargetDestination targetDestination, String subscriptionName,
            String messageSelector, int transactionalMode) {
        Destination destination = createDestination(targetDestination.getDestinationName(), transactionalMode);
        JMSContext context = newContext(transactionalMode);
        return new JMSV2Listener(context, destination, subscriptionName, messageSelector);
    }

    @Override
    public boolean validate() {
        // in order to validate if the connection is valid, a context is created and destroyed
        JMSContext context = null;
        try {
            context = createJMSContext();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "error while validating JMS Adapter", e);
            return false;
        } finally {
            IOUtil.closeQuietly(context);
        }
        return true;
    }

    @Override
    public void deactivate() {
        _jmsContext.setExceptionListener(null);
    }

    private JMSContext createJMSContext() {
        ConnectionFactory connectionFactory = createConnectionFactory();
        if (_settings.useAuthentication()) {
            return connectionFactory.createContext(_settings.getUsername(), _settings.getPassword(),
                    JMSContext.CLIENT_ACKNOWLEDGE);
        } else {
            return connectionFactory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        }
    }

    protected JMSContext newContext(int transactionalMode) {
        return _jmsContext.createContext(transactionalMode);
    }

    /**
     * Close the {@link javax.naming.Context} & {@link JMSContext} held by this instance
     */
    @Override
    public void close() {
        IOUtil.closeQuietly(_jmsContext);
        super.close();
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) {
        _jmsContext.setExceptionListener(exceptionListener);
    }

    @Override
    public void start() {
        _jmsContext.start();
    }
}
