// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get.strategy;

import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSReceiver;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.get.message.ReceivedMessage;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;

import javax.jms.Message;

/**
 * Base strategy for Receiving messages from JMS. This abstract class provides a factory method to get a concrete
 * instance depending on the Receive Mode configured for the operation.
 */
public interface ReceiveStrategy {

    /**
     * Factory method that returns the appropriate implementation of {@link ReceiveStrategy}, according to the operation
     * configuration present in the given connection.
     *
     * @param connection with the operation configuration.
     * @return the concrete {@link ReceiveStrategy}.
     */
    static ReceiveStrategy getStrategy(JMSOperationConnection connection) {
        ReceiveMode receiveMode = connection.getReceiveMode();

        switch (receiveMode) {
            case NO_WAIT:
                return new ReceiveNoWaitStrategy();
            case LIMITED_NUMBER_OF_MESSAGES:
                return new ReceiveLimitedStrategy(connection.getNumberOfMessages());
            case LIMITED_NUMBER_OF_MESSAGES_WITH_TIMEOUT:
                return new ReceiveLimitedWithTimeoutStrategy(connection.getTimeout(),
                        connection.getMaxNumberOfMessages());
            case UNLIMITED_NUMBER_OF_MESSAGES_WITH_TIMEOUT:
                return new ReceiveUnlimitedWithTimeoutStrategy(connection.getTimeout());
            default:
                throw new UnsupportedOperationException("unsupported receive mode: " + receiveMode);
        }
    }

    /**
     * Fetch a {@link Message} from JMS using the provided {@link JMSReceiver}.
     *
     * @param receiver to retrieve a {@link Message}.
     * @return the retrieved {@link Message}, it might be {@code null}.
     */
     Message receiveMessage(JMSReceiver receiver);

    /**
     * Fetch a message from JMS using the provided {@link JMSReceiver}
     *
     * @param adapter  to determine the message type
     * @param receiver to retrieve the message
     * @return the {@link ReceivedMessage} from JMS
     */
    default ReceivedMessage receive(GenericJndiBaseAdapter adapter, JMSReceiver receiver,
            TargetDestination targetDestination) {
        Message message = receiveMessage(receiver);
        if (message == null) {
            return ReceivedMessage.emptyMessage();
        }

        DestinationType destinationType = adapter.getDestinationType(message);
        return ReceivedMessage.wrapMessage(message, destinationType, adapter, targetDestination);
    }

    /**
     * Indicates whether the consumer of this strategy should continue invoking {@link #receiveMessage(JMSReceiver)} for
     * fetching messages or not.
     *
     * @return {@code true} if new messages should be retrieved, {@code false} otherwise.
     */
     boolean shouldContinue();
}
