// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DynamicPropertyMap;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSSender;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.LogUtil;
import com.boomi.util.StringUtil;

import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.Session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Base strategy for Sending messages to JMS. This abstract class provides factory methods to get a concrete instance
 * depending on the transaction mode configured in the operation.
 */
public abstract class SendStrategy {

    private static final Logger LOG = LogUtil.getLogger(SendStrategy.class);
    private static final long TIME_TO_LIVE_ZERO = 0L;
    protected final JMSSender _sender;

    private final GenericJndiBaseAdapter _adapter;
    private final long _documentSizeThreshold;
    private final Map<String, Destination> _destinationCache = new HashMap<>();

    SendStrategy(GenericJndiBaseAdapter adapter, JMSSender sender, long documentSizeThreshold) {
        _adapter = adapter;
        _sender = sender;
        _documentSizeThreshold = documentSizeThreshold;
    }

    /**
     * Static factory method to get a {@link SendStrategy} that process and send batches of messages within
     * transactions
     *
     * @param adapter               the JMS Adapter
     * @param sender                the JMS Sender
     * @param documentSizeThreshold the maximum allowed size for a message payload
     * @param transactionBatchSize  the number of messages in a transaction batch
     * @return an instance of {@link SendStrategy}
     */
    public static SendStrategy transactedStrategy(GenericJndiBaseAdapter adapter, JMSSender sender,
            long documentSizeThreshold, int transactionBatchSize) {
        return new TransactedSendStrategy(adapter, sender, documentSizeThreshold, transactionBatchSize);
    }

    /**
     * Static factory method to get a {@link SendStrategy} that process and send standalone messages
     *
     * @param adapter               the JMS Adapter
     * @param sender                the JMS Sender
     * @param documentSizeThreshold the maximum allowed size for a message payload
     * @return an instance of {@link SendStrategy}
     */
    public static SendStrategy simpleStrategy(GenericJndiBaseAdapter adapter, JMSSender sender,
            long documentSizeThreshold) {
        return new SimpleSendStrategy(adapter, sender, documentSizeThreshold);
    }

    /**
     * Send the given documents and add the results to the provided {@link OperationResponse}
     *
     * @param documents to be sent
     * @param response  where the results will be added
     */
    public abstract void send(Iterable<ObjectData> documents, OperationResponse response, String objectTypeId);

    private boolean isValidDocumentSize(ObjectData objectData) {
        try {
            return objectData.getDataSize() <= _documentSizeThreshold;
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "cannot get object data size, discarding message", e);
            return false;
        }
    }

    SendResult send(ObjectData document, String objectTypeId) {
        try {
            if (!isValidDocumentSize(document)) {
                return SendResult.error(document, "document larger than expected");
            }
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "cannot get object data size, discarding message", e);
            return SendResult.error(document, e);
        }
        TargetDestination targetDestination;
        if (JMSConstants.DYNAMIC_DESTINATION_ID.equals(objectTypeId)) {
            DynamicPropertyMap dynamicOperationProperties = document.getDynamicOperationProperties();
            targetDestination = _adapter.createTargetDestination(
                    dynamicOperationProperties.getProperty(JMSConstants.PROPERTY_DESTINATION),
                    DestinationType.fromValue(
                            dynamicOperationProperties.getProperty(JMSConstants.PROPERTY_DESTINATION_TYPE)));
        } else {
            targetDestination = _adapter.createTargetDestination(objectTypeId);
        }

        try {
            Destination destination = getDestination(_adapter, targetDestination.getDestinationName());

            Message message = SendMessageFactory.createMessage(_adapter, _sender,
                    targetDestination.getDestinationType(), document, targetDestination);
            _sender.send(destination, message, getTimeToLive(document.getDynamicOperationProperties()));

            String messageID = message.getJMSMessageID();
            SendMessageMetadata messageMetadata = new SendMessageMetadata(messageID, destination.toString(),
                    targetDestination.getDestinationType());

            return SendResult.success(document, messageMetadata);
        } catch (Exception e) {
            String message = "error sending message to " + targetDestination.getDestinationName();
            LOG.log(Level.WARNING, message, e);
            return SendResult.error(document, e);
        }
    }

    private Destination getDestination(GenericJndiBaseAdapter adapter, String destinationName) {
        return _destinationCache.computeIfAbsent(destinationName,
                key -> adapter.createDestination(key, Session.CLIENT_ACKNOWLEDGE));
    }

    private static long getTimeToLive(DynamicPropertyMap property) {
        String timeToLive = property.getProperty(JMSConstants.PROPERTY_TIME_TO_LIVE);
        if (StringUtil.isBlank(timeToLive)) {
            return TIME_TO_LIVE_ZERO;
        }
        long timeToLiveLong = 0;
        try {
            timeToLiveLong = Long.parseLong(timeToLive);
        } catch (NumberFormatException e) {
            throw new ConnectorException("Time to Live have to be a number.", e);
        }
        if (timeToLiveLong < TIME_TO_LIVE_ZERO) {
            throw new IllegalArgumentException("Expiration Time have to be greater or equal to zero.");
        }
        return timeToLiveLong;
    }
}
