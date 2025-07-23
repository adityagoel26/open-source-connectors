// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen.strategy;

import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;

import javax.jms.Message;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class responsible to handle AtLeastOnce delivery policy.
 */

public class MessageListenerAtLeastOnceStrategy extends MessageListenerStrategy {

    public MessageListenerAtLeastOnceStrategy(Listener listener, JMSListener client, GenericJndiBaseAdapter adapter,
            boolean isSingleton, TargetDestination targetDestination) {
        super(listener, adapter, client, isSingleton, targetDestination);
    }

    @Override
    protected void postAction(Message message, Future<ListenerExecutionResult> execution)
            throws ExecutionException, InterruptedException {
        boolean success = false;
        try {
            ListenerExecutionResult result = execution.get();
            success = result.isSuccess();
        } finally {
            //Once the message was processed,the message is committed or acknowledged depend on it is transactional.
            if (success) {
                getClient().commit(message);
            } else {
                getClient().rollbackIfNeeded();
            }
        }
    }
}
