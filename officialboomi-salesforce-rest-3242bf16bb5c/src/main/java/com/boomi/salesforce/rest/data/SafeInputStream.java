// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.data;

import java.io.IOException;
import java.io.InputStream;
/*
 * SafeInputStream will make sure reading throws exception if record length is more than 1,000,000 characters
 */
public class SafeInputStream extends InputStream {
    // OpenCSV Buffer Size 8192
    private static final int LIMIT = 1_000_000;
    private final InputStream _inputStream;
    private int _bytesRead;

    public SafeInputStream(InputStream inputStream) {
        _inputStream = inputStream;
        _bytesRead = 0;
    }

    @Override
    public int read() throws IOException {
        if (_bytesRead++ > LIMIT) {
            throw new IOException("Characters limits exceeded 1,000,000 characters");
        }
        return _inputStream.read();
    }

    public void resetMemoryCounter() {
        _bytesRead = 0;
    }
}
