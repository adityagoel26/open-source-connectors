// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil.doubles;

import com.boomi.util.StringUtil;

import javax.jms.MapMessage;

import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class MapMessageDouble extends MessageDouble implements MapMessage {

    private final Map<String, Object> _values;

    public MapMessageDouble(String destination, Map<String, Object> values) {
        super(destination);
        _values = values;
    }

    public MapMessageDouble() {
        this(StringUtil.EMPTY_STRING, Collections.<String, Object>emptyMap());
    }

    @Override
    public boolean getBoolean(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte getByte(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public char getChar(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public float getFloat(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double getDouble(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getString(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getBytes(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObject(String name) {
        return _values.get(name);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Enumeration getMapNames() {
        final Set<String> keys = _values.keySet();
        return new Enumeration() {
            private final Iterator<String> _iterator = keys.iterator();

            @Override
            public boolean hasMoreElements() {
                return _iterator.hasNext();
            }

            @Override
            public Object nextElement() {
                return _iterator.next();
            }
        };
    }

    @Override
    public void setBoolean(String name, boolean value) {
        _values.put(name, value);
    }

    @Override
    public void setByte(String name, byte value) {
        _values.put(name, value);
    }

    @Override
    public void setShort(String name, short value) {
        _values.put(name, value);
    }

    @Override
    public void setChar(String name, char value) {
        _values.put(name, value);
    }

    @Override
    public void setInt(String name, int value) {
        _values.put(name, value);
    }

    @Override
    public void setLong(String name, long value) {
        _values.put(name, value);
    }

    @Override
    public void setFloat(String name, float value) {
        _values.put(name, value);
    }

    @Override
    public void setDouble(String name, double value) {
        _values.put(name, value);
    }

    @Override
    public void setString(String name, String value) {
        _values.put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) {
        _values.put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) {
        _values.put(name, value);
    }

    @Override
    public void setObject(String name, Object value) {
        _values.put(name, value);
    }

    @Override
    public boolean itemExists(String name) {
        throw new UnsupportedOperationException();
    }
}
