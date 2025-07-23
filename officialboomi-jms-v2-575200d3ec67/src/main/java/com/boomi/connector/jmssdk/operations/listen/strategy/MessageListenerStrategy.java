// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen.strategy;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.api.listen.SubmitOptions;
import com.boomi.connector.api.listen.options.DistributionMode;
import com.boomi.connector.api.listen.options.WaitMode;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.get.message.ReceivedMessage;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.LogUtil;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * abstract class used to define the delivery policy behavior
 */
public abstract class MessageListenerStrategy implements MessageListener {

    private static final Logger LOG = LogUtil.getLogger(MessageListenerStrategy.class);
    private final SubmitOptions _processSubmitOptions;

    private final Listener _listener;
    private final GenericJndiBaseAdapter _adapter;
    private final JMSListener _client;
    private final TargetDestination _targetDestination;

    /**
     * Constructor of MessageListenerStrategy
     *
     * @param listener
     * @param adapter
     * @param isSingleton if it's true the listener will be executed on a single node so the processes will be executed
     *                    the remaining node, if it's false the listener will be executed how the atom is configured.
     */
    MessageListenerStrategy(Listener listener, GenericJndiBaseAdapter adapter, JMSListener client, boolean isSingleton,
            TargetDestination targetDestination) {
        _listener = listener;
        _adapter = adapter;
        _processSubmitOptions = new SubmitOptions().withWaitMode(WaitMode.PROCESS_COMPLETION);
        if (isSingleton) {
            _processSubmitOptions.withDistributionMode(DistributionMode.PREFER_REMOTE);
        }
        _client = client;
        _targetDestination = targetDestination;
    }

    /**
     * Get an instance of MessageListener depending on delivery Policy
     *
     * @param deliveryPolicy AT_LEAST_ONCE | AT_MOST_ONCE
     * @param listener
     * @param client
     * @param adapter
     * @param isSingleton    if it's true the listener will be executed on a single node so the processes will be
     *                       executed the remaining node, if it's false the listener will be executed how the atom is
     *                       configured.
     * @return
     */
    public static MessageListenerStrategy getInstance(String deliveryPolicy, Listener listener, JMSListener client,
            GenericJndiBaseAdapter adapter, boolean isSingleton, TargetDestination targetDestination) {
        if (JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE.equals(deliveryPolicy)) {
            return new MessageListenerAtLeastOnceStrategy(listener, client, adapter, isSingleton, targetDestination);
        } else if (JMSConstants.DELIVERY_POLICY_AT_MOST_ONCE.equals(deliveryPolicy)) {
            return new MessageListenerAtMostOnceStrategy(listener, client, adapter, isSingleton, targetDestination);
        } else {
            throw new ConnectorException(String.format("Delivery Policy %s is not supported", deliveryPolicy));
        }
    }

    /**
     * onMessage is called when the listener receives a message and submits the message to the process.
     *
     * @param message
     */
    @Override
    public void onMessage(Message message) {
        if (Objects.isNull(message)) {
            _listener.submit(new ConnectorException("Message received was null"));
            return;
        }
        ReceivedMessage receivedMessage = ReceivedMessage.wrapMessage(message, _adapter.getDestinationType(message),
                _adapter, _targetDestination);
        try {
            Future<ListenerExecutionResult> submit = _listener.submit(
                    receivedMessage.toPayload(_listener.createMetadata()), _processSubmitOptions);
            postAction(message, submit);
        } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Interruption detected processing message", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Failed to process message", e);
        }
    }

    /**
     * return a jmsLister
     *
     * @return jmsLister @{@link JMSListener}
     */
    protected JMSListener getClient() {
        return _client;
    }

    /**
     * the method defines the way in which the message will be handled after that was submitted to the process
     *
     * @param message
     * @param execution
     * @throws ExecutionException
     * @throws InterruptedException
     * @throws JMSException
     */
    protected abstract void postAction(Message message, Future<ListenerExecutionResult> execution)
            throws ExecutionException, InterruptedException, JMSException;
}
