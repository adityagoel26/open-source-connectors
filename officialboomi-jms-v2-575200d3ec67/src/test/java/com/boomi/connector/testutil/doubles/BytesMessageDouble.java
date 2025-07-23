// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.testutil.doubles;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.StringUtil;

import javax.jms.BytesMessage;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class BytesMessageDouble extends MessageDouble implements BytesMessage {

    private final byte[] _payload;
    private final ByteArrayInputStream _stream;

    public BytesMessageDouble(String destination, byte[] payload) {
        super(destination);
        _payload = payload;
        _stream = new ByteArrayInputStream(payload);
    }

    public BytesMessageDouble() {
        this(StringUtil.EMPTY_STRING, new byte[] {});
    }

    @Override
    public long getBodyLength() {
        return _payload.length;
    }

    @Override
    public boolean readBoolean() {
        throw new UnsupportedOperationException();
    }

    @Override
    public byte readByte() {
        return (byte) _stream.read();
    }

    @Override
    public int readUnsignedByte() {
        return _stream.read();
    }

    @Override
    public short readShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readUnsignedShort() {
        throw new UnsupportedOperationException();
    }

    @Override
    public char readChar() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readInt() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long readLong() {
        throw new UnsupportedOperationException();
    }

    @Override
    public float readFloat() {
        throw new UnsupportedOperationException();
    }

    @Override
    public double readDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String readUTF() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int readBytes(byte[] value) {
        try {
            return _stream.read(value);
        } catch (IOException e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    public int readBytes(byte[] value, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBoolean(boolean value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeByte(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeShort(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeChar(char value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeFloat(float value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeDouble(double value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeUTF(String value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(byte[] value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeBytes(byte[] value, int offset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void writeObject(Object value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }
}
