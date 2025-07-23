// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import javax.jms.BytesMessage;
import javax.jms.JMSException;

import java.io.IOException;
import java.io.InputStream;

/**
 * This class acts as facade for reading a {@link BytesMessage} as an {@link InputStream}
 */
public class BytesMessageInputStream extends InputStream {

    private final BytesMessage _message;
    private long _pendingBytes;

    public BytesMessageInputStream(BytesMessage message) throws JMSException {
        _message = message;
        _pendingBytes = message.getBodyLength();
    }

    @Override
    public int available() {
        return (_pendingBytes > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) _pendingBytes;
    }

    @Override
    public int read() throws IOException {
        if (_pendingBytes <= 0) {
            return -1;
        }

        try {
            int nextByte = _message.readUnsignedByte();
            _pendingBytes--;
            return nextByte;
        } catch (JMSException e) {
            throw new IOException(e);
        }
    }
}
