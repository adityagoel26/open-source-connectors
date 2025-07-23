// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.jmssdk.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.CollectionUtil;
import com.boomi.util.LogUtil;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Util class with handy static methods
 */
public final class Utils {

    private static final Logger LOG = LogUtil.getLogger(Utils.class);

    private Utils() {
    }

    /**
     * Quietly close the given {@link Context} without propagating any {@link Exception}
     *
     * @param context to be closed
     */
    public static void closeQuietly(Context context) {
        if (context == null) {
            return;
        }

        try {
            context.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened closing the context", e);
        }
    }

    /**
     * Quietly close the given {@link Session} without propagating any {@link Exception}.
     * <p>
     * This method does not rely on {@link com.boomi.util.IOUtil#closeQuietly(AutoCloseable...)} because {@link Session}
     * started extending {@link AutoCloseable} since Java 7 ─ meaning that invoking the aforementioned method could
     * potentially trigger a runtime error when dealing with older libraries.
     *
     * @param session to be closed
     */
    public static void closeQuietly(Session session) {
        if (session == null) {
            return;
        }

        try {
            session.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened closing the session", e);
        }
    }

    /**
     * Quietly close the given {@link MessageProducer} without propagating any {@link Exception}.
     * <p>
     * This method does not rely on {@link com.boomi.util.IOUtil#closeQuietly(AutoCloseable...)} because {@link
     * MessageProducer} started extending {@link AutoCloseable} since Java 7 ─ meaning that invoking the aforementioned
     * method could potentially trigger a runtime error when dealing with older libraries.
     *
     * @param jmsProducer to be closed
     */
    public static void closeQuietly(MessageProducer jmsProducer) {
        if (jmsProducer == null) {
            return;
        }

        try {
            jmsProducer.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened closing the producer", e);
        }
    }

    /**
     * Quietly close the given {@link MessageProducer} without propagating any {@link Exception}.
     * <p>
     * This method does not rely on {@link com.boomi.util.IOUtil#closeQuietly(AutoCloseable...)} because {@link
     * MessageProducer} started extending {@link AutoCloseable} since Java 7 ─ meaning that invoking the aforementioned
     * method could potentially trigger a runtime error when dealing with older libraries.
     *
     * @param jmsConsumer to be closed
     */
    public static void closeQuietly(MessageConsumer jmsConsumer) {
        if (jmsConsumer == null) {
            return;
        }

        try {
            jmsConsumer.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened closing the consumer", e);
        }
    }

    /**
     * Quietly close the given {@link Connection} without propagating any {@link Exception}.
     * <p>
     * This method does not rely on {@link com.boomi.util.IOUtil#closeQuietly(AutoCloseable...)} because {@link
     * Connection} started extending {@link AutoCloseable} since Java 7 ─ meaning that invoking the aforementioned
     * method could potentially trigger a runtime error when dealing with older libraries.
     *
     * @param connection to be closed
     */
    public static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened closing the connection", e);
        }
    }

    /**
     * Verify if the given {@link Iterable} contains at least one element or not
     *
     * @param iterable to be evaluated, can be null
     * @return {@code true} if the iterable is empty or null, {@code false} otherwise
     */
    public static boolean isEmpty(Iterable<?> iterable) {
        if (iterable == null) {
            return true;
        }

        Iterator<?> iterator = iterable.iterator();
        return !iterator.hasNext();
    }

    /**
     * Assert that the given {@code Object} is an {@code instanceof} the given {@code type}. If the assertion is
     * correct, this method return the original object casted to the given type, otherwise a {@link ConnectorException}
     * is thrown.
     *
     * @param object the object to be evaluated
     * @param type   the type expected for the object
     * @return a reference of type U for the given object
     */
    public static <T, U extends T> U validateAndCast(T object, Class<U> type) {
        if (type.isInstance(object)) {
            return type.cast(object);
        }

        throw new ConnectorException(String.format("expected %s but received %s", type, object.getClass()));
    }

    /**
     * Convert the given Enumeration to a Iterable of Strings
     *
     * @param enumeration the enumeration
     * @return an Iterable of Strings
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Iterable<String> toIterable(Enumeration enumeration) {
        return CollectionUtil.toIterable(enumeration);
    }

    /**
     * This method accepts any {@link Map} and guarantees that the return object is not null.
     *
     * @param map to be validated
     * @return the same map or an empty one if it was {@code null}
     */
    public static <K, V> Map<K, V> nullSafe(Map<K, V> map) {
        if (map == null) {
            return Collections.emptyMap();
        }
        return map;
    }

    /**
     * Extract the priority from the given message. If an error happens, {@link Message#DEFAULT_PRIORITY}.
     *
     * @param message to extract the priority
     * @return the message priority
     */
    public static int getMessagePriority(Message message) {
        try {
            return message.getJMSPriority();
        } catch (JMSException e) {
            LOG.log(Level.WARNING, "cannot obtain message priority", e);
            return Message.DEFAULT_PRIORITY;
        }
    }
}
