// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSReceiver;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.get.message.ReceivedMessage;
import com.boomi.connector.jmssdk.operations.get.strategy.ReceiveStrategy;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.connector.util.BaseQueryOperation;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import java.util.logging.Level;
import java.util.logging.Logger;

public class JMSGetOperation extends BaseQueryOperation {

    private static final Logger LOG = LogUtil.getLogger(JMSGetOperation.class);

    private final boolean _useDurableSubscription;
    private final String _subscriptionName;
    private final int _transactionalMode;

    public JMSGetOperation(JMSOperationConnection connection) {
        super(connection);
        _useDurableSubscription = connection.useSubscription();
        _subscriptionName = connection.getSubscriptionName();
        _transactionalMode = connection.getTransactionMode();
    }

    private JMSReceiver createReceiver(GenericJndiBaseAdapter adapter, TargetDestination targetDestination,
            String messageSelector) {
        JMSReceiver jmsReceiver = null;
        if (_useDurableSubscription) {
            jmsReceiver = adapter.createReceiver(targetDestination, _subscriptionName, messageSelector,
                    _transactionalMode);
        } else {
            jmsReceiver = adapter.createReceiver(targetDestination, messageSelector, _transactionalMode);
        }
        adapter.start();
        return jmsReceiver;
    }

    @Override
    protected void executeQuery(QueryRequest request, OperationResponse response) {
        FilterData filter = request.getFilter();
        GetResponseHandler responseHandler = new GetResponseHandler(filter, response);

        AdapterPool adapterPool = AdapterPoolManager.getPool(getConnection());
        GenericJndiBaseAdapter adapter = null;
        JMSReceiver receiver = null;
        try {
            adapter = adapterPool.createAdapter();
            TargetDestination targetDestination = getDestination(filter, adapter);
            String messageSelector = FilterProcessor.getMessageSelector(request.getFilter());
            receiver = createReceiver(adapter, targetDestination, messageSelector);

            ReceiveStrategy receiveStrategy = ReceiveStrategy.getStrategy(getConnection());
            receiveMessages(adapter, receiver, responseHandler, receiveStrategy, targetDestination);
        } catch (Exception e) {
            responseHandler.addError(e);
        } finally {
            IOUtil.closeQuietly(responseHandler, receiver);
            adapterPool.releaseAdapter(adapter);
        }
    }

    private TargetDestination getDestination(TrackedData filter, GenericJndiBaseAdapter adapter) {
        TargetDestination targetDestination;
        if (JMSConstants.DYNAMIC_DESTINATION_ID.equals(getContext().getObjectTypeId())) {
            DynamicPropertyMap dynamicOperationProperties = filter.getDynamicOperationProperties();
            targetDestination = adapter.createAndLoadTargetDestination(
                    dynamicOperationProperties.getProperty(JMSConstants.PROPERTY_DESTINATION));
        } else {
            targetDestination = adapter.createTargetDestination(getContext().getObjectTypeId());
        }
        return targetDestination;
    }

    private void receiveMessages(GenericJndiBaseAdapter adapter, JMSReceiver receiver,
            GetResponseHandler responseHandler, ReceiveStrategy receiveStrategy, TargetDestination targetDestination) {

        do {
            ReceivedMessage message = receiveStrategy.receive(adapter, receiver, targetDestination);
            if (!message.hasMessage()) {
                continue;
            }

            Payload payload = message.toPayload(getContext().createMetadata());
            try {
                receiver.commit();
                responseHandler.addSuccess(payload);
            } catch (RuntimeException e) {
                String errorMessage = "Failed to commit message: " + e.getMessage();
                LOG.log(Level.WARNING, errorMessage, e);
                responseHandler.addError(payload, errorMessage);
            }
        } while (receiveStrategy.shouldContinue());
    }

    @Override
    public JMSOperationConnection getConnection() {
        return (JMSOperationConnection) super.getConnection();
    }
}
