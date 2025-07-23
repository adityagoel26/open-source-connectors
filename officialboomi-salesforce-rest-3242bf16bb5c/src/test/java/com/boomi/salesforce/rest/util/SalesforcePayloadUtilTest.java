// Copyright (c) 2025 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.util.BasePayload;
import com.boomi.connector.api.Payload;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class SalesforcePayloadUtilTest {

    @Test
    void payloadToInputStreamUsingReadFrom() {
        String content = "the expected content";
        Payload payload = new BasePayload() {
            @Override
            public InputStream readFrom() {
                return new ByteArrayInputStream(content.getBytes(StringUtil.UTF8_CHARSET));
            }
        };

        try (InputStream stream = SalesforcePayloadUtil.payloadToInputStream(payload)) {
            Assertions.assertEquals(content, StreamUtil.toString(stream, StringUtil.UTF8_CHARSET));
        } catch (IOException e) {
            Assertions.fail(e);
        }
    }

    @Test
    void payloadToInputStreamUsingWriteTo() {
        String content = "the expected content";
        Payload payload = new BasePayload() {
            @Override
            public void writeTo(OutputStream out) throws IOException {
                out.write(content.getBytes(StringUtil.UTF8_CHARSET));
            }
        };

        try (InputStream stream = SalesforcePayloadUtil.payloadToInputStream(payload)) {
            Assertions.assertEquals(content, StreamUtil.toString(stream, StringUtil.UTF8_CHARSET));
        } catch (IOException e) {
            Assertions.fail(e);
        }
    }
}
