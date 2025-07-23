// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.listen.ListenManager;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.SingletonListenOperation;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerStrategy;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.util.listen.UnmanagedListenOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import javax.jms.ExceptionListener;
import javax.jms.JMSException;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JMSListenOperation extends UnmanagedListenOperation
        implements SingletonListenOperation<ListenManager>, ExceptionListener {

    private static final Logger LOG = LogUtil.getLogger(JMSListenOperation.class);
    private static final String ERROR_JMS_LISTENER_CANNOT_START = "JMS Listener cannot start";
    private GenericJndiBaseAdapter _adapter;
    private final List<JMSListener> _instances;
    private final int _maxConcurrentExecutions;
    private final boolean _useSubscription;
    private final boolean _isJmsV1;
    private final String _deliveryPolicy;
    private final String _subscriptionName;
    private final String _messageSelector;
    private final int _transactionalMode;
    private Listener _listener;

    public JMSListenOperation(JMSOperationConnection connection) {
        super(connection);
        _maxConcurrentExecutions = connection.getMaxConcurrentExecutions();
        _instances = new LinkedList<>();
        _useSubscription = connection.useSubscription();
        _isJmsV1 = connection.getJMSVersion() == JMSConstants.JMSVersion.V1_1;
        _deliveryPolicy = connection.getDeliveryPolicy();
        _subscriptionName = connection.getSubscriptionName();
        _messageSelector = connection.getMessageSelector();
        _transactionalMode = connection.getTransactionMode();
    }

    /**
     * Start the listener and create the jms client according to max concurrent executions.
     *
     * @param listener
     */
    @Override
    protected void start(Listener listener) {
        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());
        JMSListener client = null;
        try {
            _adapter = adapterPool.createAdapter();
            _adapter.setExceptionListener(this);
            TargetDestination targetDestination = getDestination();
            _listener = listener;
            for (int concurrentCount = 0; concurrentCount < _maxConcurrentExecutions; ++concurrentCount) {
                if (_useSubscription) {
                    client = _adapter.createListener(targetDestination, _subscriptionName, _messageSelector,
                            _transactionalMode);
                } else {
                    client = _adapter.createListener(targetDestination, _messageSelector, _transactionalMode);
                }
                client.subscribeConsumer(
                        MessageListenerStrategy.getInstance(_deliveryPolicy, listener, client, _adapter, isSingleton(),
                                targetDestination));
                _instances.add(client);
                //JMSV1 just supports a single consumer when the destination is a topic.
                if (client.isListeningFromTopic() && _isJmsV1) {
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.log(Level.FINE, "JMSV1 just supports a single consumer when the destination is a topic.");
                    }
                    break;
                }
            }
            _adapter.start();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, ERROR_JMS_LISTENER_CANNOT_START, e);
            IOUtil.closeQuietly(client);
            stop();
            throw new ConnectorException(ERROR_JMS_LISTENER_CANNOT_START, e);
        }
    }

    /**
     * free and close the resources.
     */
    @Override
    public void stop() {
        IOUtil.closeQuietly(_instances);
        _instances.clear();
        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());
        adapterPool.releaseAdapter(_adapter);
    }

    @Override
    public JMSOperationConnection getConnection() {
        return (JMSOperationConnection) super.getConnection();
    }

    @Override
    public boolean isSingleton() {
        return getConnection().getSingletonListener();
    }

    /**
     * the method is called when jms listen throw an exception,it logs and submits it
     *
     * @param e @{@link JMSException}
     */

    @Override
    public void onException(JMSException e) {
        LOG.log(Level.WARNING, "JMS Listener error", e);
        _listener.submit(e);
    }

    private TargetDestination getDestination() {
        TargetDestination targetDestination = null;
        if (JMSConstants.DYNAMIC_DESTINATION_ID.equals(getContext().getObjectTypeId())) {
            targetDestination = _adapter.createTargetDestination(getConnection().getDestination(), null);
        } else {
            targetDestination = _adapter.createTargetDestination(getContext().getObjectTypeId());
        }
        return targetDestination;
    }
}
