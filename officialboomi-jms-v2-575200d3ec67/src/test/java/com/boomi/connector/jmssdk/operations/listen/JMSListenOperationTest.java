// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.listen;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.listen.Listener;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.JMSListener;
import com.boomi.connector.jmssdk.operations.JMSOperationConnection;
import com.boomi.connector.jmssdk.operations.listen.strategy.MessageListenerAtLeastOnceStrategy;
import com.boomi.connector.jmssdk.operations.model.GenericTargetDestination;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.connector.jmssdk.pool.AdapterPool;
import com.boomi.connector.jmssdk.pool.AdapterPoolManager;
import com.boomi.connector.jmssdk.util.JMSConstants;
import com.boomi.util.StringUtil;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.modules.junit4.PowerMockRunnerDelegate;

import javax.jms.Session;

import java.util.Arrays;
import java.util.Collection;

@RunWith(PowerMockRunner.class)
@PowerMockRunnerDelegate(Parameterized.class)
@PrepareForTest(AdapterPoolManager.class)
@Ignore("Temporary disabling tests using PowerMock")
public class JMSListenOperationTest {

    private static final int VALUE_MAX_CONCURRENT_EXECUTIONS = 3;

    private boolean _useSubscription;
    private boolean _isTopic;
    private int _valueMaxConcurrent;
    private JMSConstants.JMSVersion _jmsVersion;

    public JMSListenOperationTest(boolean useSubscription, boolean isTopic, int valueMaxConcurrent,
            JMSConstants.JMSVersion jmsVersion) {
        _useSubscription = useSubscription;
        _isTopic = isTopic;
        _valueMaxConcurrent = valueMaxConcurrent;
        _jmsVersion = jmsVersion;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getParameters() {
        return Arrays.asList(new Object[][] {
                { false, false, VALUE_MAX_CONCURRENT_EXECUTIONS, JMSConstants.JMSVersion.V1_1 },
                { true, false, VALUE_MAX_CONCURRENT_EXECUTIONS, JMSConstants.JMSVersion.V1_1 },
                { true, true, 1, JMSConstants.JMSVersion.V1_1 },
                { true, true, VALUE_MAX_CONCURRENT_EXECUTIONS, JMSConstants.JMSVersion.V2_0 } });
    }

    @Test
    public void startTest() {
        JMSOperationConnection connection = mockConnection();
        GenericJndiBaseAdapter adapter = Mockito.mock(GenericJndiBaseAdapter.class);
        JMSListener jmsListener = Mockito.mock(JMSListener.class);
        Mockito.when(jmsListener.isListeningFromTopic()).thenReturn(_isTopic);
        TargetDestination destination = new GenericTargetDestination("messi-queue");

        Mockito.when(adapter.createListener(destination, StringUtil.EMPTY_STRING, Session.SESSION_TRANSACTED))
                .thenReturn(jmsListener);
        Mockito.when(adapter.createListener(destination, StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING,
                Session.SESSION_TRANSACTED)).thenReturn(jmsListener);
        Mockito.when(adapter.createTargetDestination("messi-queue")).thenReturn(destination);

        PowerMockito.mockStatic(AdapterPoolManager.class);
        AdapterPool adapterPool = Mockito.mock(AdapterPool.class);
        Mockito.when(AdapterPoolManager.getPool(connection)).thenReturn(adapterPool);
        Mockito.when(adapterPool.createAdapter()).thenReturn(adapter);

        JMSListenOperation operation = new JMSListenOperation(connection);
        operation.start(Mockito.mock(Listener.class));
        if (_useSubscription) {
            Mockito.verify(adapter, Mockito.times(_valueMaxConcurrent)).createListener(destination,
                    StringUtil.EMPTY_STRING, StringUtil.EMPTY_STRING, Session.SESSION_TRANSACTED);
        } else {
            Mockito.verify(adapter, Mockito.times(_valueMaxConcurrent)).createListener(destination,
                    StringUtil.EMPTY_STRING, Session.SESSION_TRANSACTED);
        }
        Mockito.verify(jmsListener, Mockito.times(_valueMaxConcurrent)).subscribeConsumer(
                Mockito.any(MessageListenerAtLeastOnceStrategy.class));
        Mockito.verify(adapter, Mockito.times(1)).start();
    }

    @Test(expected = ConnectorException.class)
    public void startExceptionTest() {
        JMSOperationConnection connection = mockConnection();
        GenericJndiBaseAdapter adapter = Mockito.mock(GenericJndiBaseAdapter.class);

        PowerMockito.mockStatic(AdapterPoolManager.class);
        AdapterPool adapterPool = Mockito.mock(AdapterPool.class);
        Mockito.when(AdapterPoolManager.getPool(connection)).thenReturn(adapterPool);
        Mockito.when(adapterPool.createAdapter()).thenReturn(adapter);

        JMSListener jmsListener = Mockito.mock(JMSListener.class);
        Mockito.when(jmsListener.isListeningFromTopic()).thenReturn(_isTopic);
        ConnectorException toBeThrown = new ConnectorException("");
        Mockito.doThrow(toBeThrown).when(jmsListener).subscribeConsumer(Mockito.any());
        Mockito.when(adapter.createListener(new GenericTargetDestination("messi-queue"), StringUtil.EMPTY_STRING,
                Session.CLIENT_ACKNOWLEDGE)).thenReturn(jmsListener);
        Mockito.when(adapter.createListener(new GenericTargetDestination("messi-queue"), StringUtil.EMPTY_STRING,
                StringUtil.EMPTY_STRING, Session.CLIENT_ACKNOWLEDGE)).thenReturn(jmsListener);
        JMSListenOperation operation = new JMSListenOperation(connection);
        Listener listener = Mockito.mock(Listener.class);
        operation.start(listener);
    }

    private JMSOperationConnection mockConnection() {
        OperationContext context = Mockito.mock(OperationContext.class);
        Mockito.when(context.getObjectTypeId()).thenReturn("messi-queue");

        JMSOperationConnection connection = Mockito.mock(JMSOperationConnection.class);
        Mockito.when(connection.getOperationContext()).thenReturn(context);
        Mockito.when(connection.getMaxConcurrentExecutions()).thenReturn(VALUE_MAX_CONCURRENT_EXECUTIONS);
        Mockito.when(connection.getJMSVersion()).thenReturn(_jmsVersion);
        Mockito.when(connection.getDestination()).thenReturn("messi-queue");
        Mockito.when(connection.getSubscriptionName()).thenReturn(StringUtil.EMPTY_STRING);
        Mockito.when(connection.getMessageSelector()).thenReturn(StringUtil.EMPTY_STRING);
        Mockito.when(connection.getDeliveryPolicy()).thenReturn(JMSConstants.DELIVERY_POLICY_AT_LEAST_ONCE);
        Mockito.when(connection.useSubscription()).thenReturn(_useSubscription);
        return connection;
    }
}
