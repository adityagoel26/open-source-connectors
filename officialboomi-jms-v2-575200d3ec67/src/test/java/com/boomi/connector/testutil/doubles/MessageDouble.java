// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.testutil.doubles;

import javax.jms.Destination;
import javax.jms.Message;

import java.util.Collections;
import java.util.Enumeration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MessageDouble implements Message {

    private final Destination _destination = mock(Destination.class);
    private int _priority = 4;

    MessageDouble(String destination) {
        mockDestination(destination);
    }

    private void mockDestination(String destination) {
        when(_destination.toString()).thenReturn(destination);
    }

    @Override
    public String getJMSMessageID() {
        return "JMS Message ID";
    }

    @Override
    public void setJMSMessageID(String id) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJMSTimestamp() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSTimestamp(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSCorrelationID(String correlationID) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getJMSCorrelationID() {
        return "JMS Correlation ID";
    }

    @Override
    public Destination getJMSReplyTo() {
        return null;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Destination getJMSDestination() {
        return _destination;
    }

    @Override
    public void setJMSDestination(Destination destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getJMSDeliveryMode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getJMSRedelivered() {
        return false;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getJMSType() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSType(String type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJMSExpiration() {
        return 0L;
    }

    @Override
    public void setJMSExpiration(long expiration) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getJMSDeliveryTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getJMSPriority() {
        return _priority;
    }

    @Override
    public void setJMSPriority(int priority) {
        _priority = priority;
    }

    @Override
    public void clearProperties() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean propertyExists(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean getBooleanProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByteProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShortProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getIntProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLongProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloatProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDoubleProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getStringProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObjectProperty(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setBooleanProperty(String name, boolean value) {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Enumeration getPropertyNames() {
        return Collections.emptyEnumeration();
    }

    @Override
    public void setByteProperty(String name, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setShortProperty(String name, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setIntProperty(String name, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setLongProperty(String name, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setFloatProperty(String name, float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setDoubleProperty(String name, double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setStringProperty(String name, String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setObjectProperty(String name, Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledge() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clearBody() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getBody(Class<T> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isBodyAssignableTo(Class c) {
        throw new UnsupportedOperationException();
    }
}
