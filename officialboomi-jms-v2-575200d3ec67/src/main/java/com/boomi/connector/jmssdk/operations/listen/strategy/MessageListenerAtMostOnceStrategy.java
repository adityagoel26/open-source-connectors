// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen.strategy;

import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.util.LogUtil;

import javax.jms.JMSException;
import javax.jms.Message;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class responsible to handle AtMostOnce delivery policy.
 */
public class MessageListenerAtMostOnceStrategy extends MessageListenerStrategy {

    private static final Logger LOG = LogUtil.getLogger(MessageListenerAtMostOnceStrategy.class);

    public MessageListenerAtMostOnceStrategy(Listener listener, JMSListener client, GenericJndiBaseAdapter adapter,
            boolean isSingleton, TargetDestination targetDestination) {
        super(listener, adapter, client, isSingleton, targetDestination);
    }

    @Override
    protected void postAction(Message message, Future<ListenerExecutionResult> execution)
            throws ExecutionException, InterruptedException, JMSException {
        //the message is committed or acknowledged after being submitted, regardless of the processing result
        getClient().commit(message);
        // waiting to finish in order to avoid taking another message with the same consumer
        ListenerExecutionResult result = execution.get();
        if (!result.isSuccess() && LOG.isLoggable(Level.FINE)) {
            LOG.log(Level.FINE, "Message ID : {0} failed to process", message.getJMSMessageID());
        }
    }
}
