// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.Payload;
import com.boomi.util.StreamUtil;
import com.boomi.util.TestUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

public class SalesforceResponseUtilTest {

    @BeforeAll
    public static void disableLogs() {
        TestUtil.disableBoomiLog();
    }

    @Test
    void getContent() {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return new InputStreamEntity(StreamUtil.EMPTY_STREAM, ContentType.APPLICATION_JSON);
            }
        };

        InputStream content = SalesforceResponseUtil.getContent(response);
        Assertions.assertEquals(StreamUtil.EMPTY_STREAM, content);
    }

    @Test
    void getContentFromNullEntity() {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return null;
            }
        };

        InputStream content = SalesforceResponseUtil.getContent(response);
        Assertions.assertNull(content);
    }

    @Test
    void getPayload() throws IOException {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return new InputStreamEntity(StreamUtil.EMPTY_STREAM, ContentType.APPLICATION_JSON);
            }
        };

        Payload payload = SalesforceResponseUtil.toPayload(response);
        Assertions.assertEquals(StreamUtil.EMPTY_STREAM, payload.readFrom());
    }

    @Test
    void getPayloadWithNullResponse() throws IOException {
        Payload payload = SalesforceResponseUtil.toPayload(null);
        Assertions.assertNull(payload);
    }

    @Test
    void getQueryPageLocatorQuietly() {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public boolean containsHeader(String name) {
                return true;
            }

            @Override
            public Header getHeader(String name) {
                if ("Sforce-Locator".equals(name)) {
                    return new BasicHeader("Sforce-Locator", "the-locator");
                }
                throw new ConnectorException("wrong header name");
            }
        };

        String result = SalesforceResponseUtil.getQueryPageLocatorQuietly(response);
        Assertions.assertEquals("the-locator", result);
    }

    @Test
    void getQueryPageLocatorQuietlyWithNullHeaderValue() {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public boolean containsHeader(String name) {
                return true;
            }

            @Override
            public Header getHeader(String name) {
                if ("Sforce-Locator".equals(name)) {
                    return new BasicHeader("Sforce-Locator", "null");
                }
                throw new ConnectorException("wrong header name");
            }
        };

        String result = SalesforceResponseUtil.getQueryPageLocatorQuietly(response);
        Assertions.assertNull(result);
    }

    @Test
    void getQueryPageLocatorQuietlyWithNullHeaderValueWhenExceptionIsThrown() {
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public boolean containsHeader(String name) {
                return true;
            }

            @Override
            public Header getHeader(String name) {
                throw new ConnectorException("expected exception");
            }
        };

        String result = SalesforceResponseUtil.getQueryPageLocatorQuietly(response);
        Assertions.assertNull(result);
    }
}
