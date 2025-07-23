// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.operations.get.strategy.ReceiveMode;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.ByteUnit;

import javax.jms.Session;

/**
 * Implementation of {@link JMSConnection} for accessing operation configuration
 */
public class JMSOperationConnection extends JMSConnection<OperationContext> {

    public JMSOperationConnection(OperationContext context) {
        super(context);
    }

    /**
     * Indicates the transactional mode configured for the operation:
     * <ul>
     * <li>{@link Session#SESSION_TRANSACTED} if transactions are enabled.</li>
     * <li>{@link Session#AUTO_ACKNOWLEDGE} if transactions are disabled and the operation is Get, Send or Listen
     * configured as At most once.</li>
     * <li>{@link Session#CLIENT_ACKNOWLEDGE} if transactions are disabled and the operation is Listen configured as
     * At least once</li>
     * </ul>
     *
     * @return the int value representing the transactional mode.
     */
    public int getTransactionMode() {
        boolean useTransactions = getOperationProperties().getBooleanProperty(JMSConstants.PROPERTY_USE_TRANSACTION,
                false);
        if (useTransactions) {
            return Session.SESSION_TRANSACTED;
        }

        OperationType operationType = getContext().getOperationType();
        switch (operationType) {
            case QUERY:
            case CREATE:
                return Session.AUTO_ACKNOWLEDGE;
            case LISTEN:
                if (JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE.equals(getDeliveryPolicy())) {
                    return Session.CLIENT_ACKNOWLEDGE;
                } else {
                    return Session.AUTO_ACKNOWLEDGE;
                }
            default:
                throw new UnsupportedOperationException("unsupported operation type " + operationType);
        }
    }

    /**
     * Get the Batch Size configured for Send transactions.
     *
     * @return the transaction batch size configured in the operation, or 1 if the value is not defined.
     */
    public int getTransactionBatchSize() {
        return toInt(getOperationProperties().getLongProperty(JMSConstants.PROPERTY_TRANSACTION_BATCH_SIZE, 1L));
    }

    /**
     * Get the maximum document size supported by the container for send messages.
     *
     * @return the maximum document size configured, or 1 MB if the value is not defined.
     */
    public long getDocumentSizeThreshold() {
        long defaultSize = ByteUnit.byteSize(1, ByteUnit.MB.name());
        return getContext().getConfig().getLongContainerProperty(JMSConstants.PROPERTY_DOCUMENT_SIZE_THRESHOLD,
                defaultSize);
    }

    /**
     * Indicates whether the operation is configured to use subscriptions or not.
     *
     * @return {@code true} if subscriptions are enabled, {@code false} otherwise.
     */
    public boolean useSubscription() {
        return getOperationProperties().getBooleanProperty(JMSConstants.PROPERTY_USE_DURABLE_SUBSCRIPTION, false);
    }

    /**
     * Get the configured Subscription Name.
     *
     * @return the subscription name.
     */
    public String getSubscriptionName() {
        return getOperationProperties().getProperty(JMSConstants.PROPERTY_SUBSCRIPTION_NAME);
    }

    /**
     * Get the configured Receive Mode.
     *
     * @return the receive mode.
     */
    public ReceiveMode getReceiveMode() {
        return ReceiveMode.valueOf(getOperationProperties().getProperty(JMSConstants.PROPERTY_RECEIVE_MODE));
    }

    /**
     * Get the configured timeout in milliseconds.
     *
     * @return the timeout.
     */
    public long getTimeout() {
        return getOperationProperties().getLongProperty(JMSConstants.PROPERTY_RECEIVE_TIMEOUT);
    }

    /**
     * Get the configured Number of Messages that should be retrieved from JMS.
     *
     * @return the number of messages.
     */
    public long getNumberOfMessages() {
        return getOperationProperties().getLongProperty(JMSConstants.PROPERTY_NUMBER_OF_MESSAGES);
    }

    /**
     * Get the configured Maximum Number of Messages that should be retrieved from JMS.
     *
     * @return the maximum number of messages.
     */
    public long getMaxNumberOfMessages() {
        return getOperationProperties().getLongProperty(JMSConstants.PROPERTY_MAXIMUM_NUMBER_OF_MESSAGES);
    }

    public String getDestination() {
        return getOperationProperties().getProperty(JMSConstants.PROPERTY_DESTINATION);
    }

    public String getMessageSelector() {
        return getOperationProperties().getProperty(JMSConstants.PROPERTY_MESSAGE_SELECTOR);
    }

    public int getMaxConcurrentExecutions() {
        return toInt(getOperationProperties().getLongProperty(JMSConstants.PROPERTY_MAX_CONCURRENT_EXECUTIONS, 1L));
    }

    private static int toInt(Long value) {
        if (value > Integer.MAX_VALUE || value < 1) {
            throw new ConnectorException(String.format("exceeded max value: %s ", value));
        }

        return value.intValue();
    }

    public boolean getSingletonListener() {
        return getOperationProperties().getBooleanProperty(JMSConstants.PROPERTY_SINGLETON_LISTENER, false);
    }

    public String getDeliveryPolicy() {
        return getOperationProperties().getProperty(JMSConstants.PROPERTY_DELIVERY_POLICY,
                JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE);
    }

    private PropertyMap getOperationProperties() {
        return getContext().getOperationProperties();
    }
}
