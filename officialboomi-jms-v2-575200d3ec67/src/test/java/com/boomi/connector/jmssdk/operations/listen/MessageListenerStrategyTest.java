// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen;

import com.boomi.connector.api.Payload;
import com.boomi.connector.api.PayloadMetadata;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.api.listen.ListenerExecutionResult;
import com.boomi.connector.jmssdk.client.DestinationType;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.get.message.ReceivedMessage;
import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerStrategy;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.util.JMSConstants;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageListenerStrategyTest {

    @Test
    public void onMessage_AT_LEAST_ONCE_Success_Test() throws Exception {
        Listener listener = Mockito.mock(Listener.class);
        Future<ListenerExecutionResult> future = mock(FutureTask.class);
        TextMessage message = Mockito.mock(TextMessage.class);
        JMSListener client = Mockito.mock(JMSListener.class);
        prepareTest(JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE, listener, client, future, message, true);
        InOrder inOrder = Mockito.inOrder(listener, future, client);
        inOrder.verify(listener).submit(Mockito.any(), Mockito.any());
        inOrder.verify(future).get();
        inOrder.verify(client).commit(message);
    }

    @Test
    public void onMessage_AT_LEAST_ONCE_Fail_Test() throws Exception {
        Listener listener = Mockito.mock(Listener.class);
        Future<ListenerExecutionResult> future = mock(FutureTask.class);
        TextMessage message = Mockito.mock(TextMessage.class);
        JMSListener client = Mockito.mock(JMSListener.class);
        prepareTest(JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE, listener, client, future, message, false);
        InOrder inOrder = Mockito.inOrder(listener, future, client);
        inOrder.verify(listener).submit(Mockito.any(), Mockito.any());
        inOrder.verify(future).get();
        inOrder.verify(client).rollbackIfNeeded();
    }

    @Test
    public void onMessage_AT_MOST_ONCE_Success_Test() throws Exception {
        Listener listener = Mockito.mock(Listener.class);
        Future<ListenerExecutionResult> future = mock(FutureTask.class);
        TextMessage message = Mockito.mock(TextMessage.class);
        JMSListener client = Mockito.mock(JMSListener.class);
        prepareTest(JMSConstants.DELIVERY_POLICY_AT_MOST_ONCE, listener, client, future, message, true);
        InOrder inOrder = Mockito.inOrder(listener, future, client);
        inOrder.verify(listener).submit(Mockito.any(), Mockito.any());
        inOrder.verify(client).commit(message);
        inOrder.verify(future).get();
    }

    private void prepareTest(String deliveryPolicy, Listener listener, JMSListener client,
            Future<ListenerExecutionResult> future, TextMessage message, boolean processSuccess)
            throws JMSException, ExecutionException, InterruptedException {
        GenericJndiBaseAdapter adapter = Mockito.mock(GenericJndiBaseAdapter.class);
        Mockito.when(adapter.getDestinationType(Mockito.any(Message.class))).thenReturn(DestinationType.TEXT_MESSAGE);
        ListenerExecutionResult execute = createListenerResult(processSuccess);
        when(future.get()).thenReturn(execute);
        Mockito.when(listener.submit(Mockito.any(), Mockito.any())).thenReturn(future);

        Mockito.when(listener.createMetadata()).thenReturn(Mockito.mock(PayloadMetadata.class));
        MessageListenerStrategy messageListener = MessageListenerStrategy.getInstance(deliveryPolicy, listener, client,
                adapter, false,Mockito.mock(TargetDestination.class));

        Mockito.when(message.getText()).thenReturn("<?xml version=\"1.0\"?><message>Unit Test Message</message>");
        Mockito.when(message.getJMSDestination()).thenReturn(Mockito.mock(Destination.class));
        ReceivedMessage receivedMessageMock = Mockito.mock(ReceivedMessage.class);
        Mockito.when(receivedMessageMock.toPayload(Mockito.any(PayloadMetadata.class))).thenReturn(
                Mockito.mock(Payload.class));
        messageListener.onMessage(message);
    }

    public static ListenerExecutionResult createListenerResult(final boolean success) {
        return new ListenerExecutionResult() {
            @Override
            public String getExecutionId() {
                return null;
            }

            @Override
            public boolean isSuccess() {
                return success;
            }
        };
    }
}
