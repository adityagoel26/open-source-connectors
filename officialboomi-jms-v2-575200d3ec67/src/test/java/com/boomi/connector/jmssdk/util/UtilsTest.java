// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.util;

import com.boomi.connector.testutil.doubles.TextMessageDouble;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.NamingException;

public class UtilsTest {

    private static final VerificationMode INVOKED_ONCE = Mockito.times(1);

    @Test
    public void closeQuietlyNullContextWithoutErrorsTest() {
        Utils.closeQuietly((Context) null);
    }

    @Test
    public void closeQuietlyNullConnectionWithoutErrorsTest() {
        Utils.closeQuietly((Connection) null);
    }

    @Test
    public void closeQuietlyNullSessionWithoutErrorsTest() {
        Utils.closeQuietly((Session) null);
    }

    @Test
    public void closeQuietlyContextWithoutErrorsWhenAnExceptionIsThrownTest() throws NamingException {
        Context context = Mockito.mock(Context.class);
        Mockito.doThrow(RuntimeException.class).when(context).close();

        Utils.closeQuietly(context);
        Mockito.verify(context, INVOKED_ONCE).close();
    }

    @Test
    public void closeQuietlyConnectionWithoutErrorsWhenAnExceptionIsThrownTest() throws JMSException {
        Connection connection = Mockito.mock(Connection.class);
        Mockito.doThrow(RuntimeException.class).when(connection).close();

        Utils.closeQuietly(connection);
        Mockito.verify(connection, INVOKED_ONCE).close();
    }

    @Test
    public void closeQuietlySessionWithoutErrorsWhenAnExceptionIsThrownTest() throws JMSException {
        Session session = Mockito.mock(Session.class);
        Mockito.doThrow(RuntimeException.class).when(session).close();

        Utils.closeQuietly(session);
        Mockito.verify(session, INVOKED_ONCE).close();
    }

    @Test
    public void closeQuietlyContextTest() throws NamingException {
        Context context = Mockito.mock(Context.class);

        Utils.closeQuietly(context);
        Mockito.verify(context, INVOKED_ONCE).close();
    }

    @Test
    public void closeQuietlyConnectionTest() throws JMSException {
        Connection connection = Mockito.mock(Connection.class);

        Utils.closeQuietly(connection);
        Mockito.verify(connection, INVOKED_ONCE).close();
    }

    @Test
    public void closeQuietlySessionTest() throws JMSException {
        Session session = Mockito.mock(Session.class);

        Utils.closeQuietly(session);
        Mockito.verify(session, INVOKED_ONCE).close();
    }

    @Test
    public void getMessagePriorityTest() throws JMSException {
        Message message = new TextMessageDouble();

        int expectedPriority = 9;
        message.setJMSPriority(expectedPriority);

        Assert.assertEquals(expectedPriority, Utils.getMessagePriority(message));
    }

    @Test
    public void getDefaultMessagePriorityOnErrorTest() throws JMSException {
        Message message = Mockito.mock(Message.class);
        Mockito.when(message.getJMSPriority()).thenThrow(new JMSException("error"));

        Assert.assertEquals(Message.DEFAULT_PRIORITY, Utils.getMessagePriority(message));
    }
}
