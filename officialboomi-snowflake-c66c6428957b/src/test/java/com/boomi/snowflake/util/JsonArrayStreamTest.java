// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;


public class JsonArrayStreamTest {

    private JsonArrayStream jsonArrayStream;

    @Test
    public void testFirstCallReturnsLeftBracket() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("test\ninput".getBytes());
        jsonArrayStream = new JsonArrayStream(inputStream);

        int firstChar = jsonArrayStream.read();
        Assert.assertEquals('[', firstChar);
    }

    @Test
    public void testEmptyInputStream() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("".getBytes());
        jsonArrayStream = new JsonArrayStream(inputStream);

        // Should immediately return '[' and then ']'
        Assert.assertEquals('[', jsonArrayStream.read());
        Assert.assertEquals(']', jsonArrayStream.read());
        Assert.assertEquals(-1, jsonArrayStream.read());  // EOF
    }
}
