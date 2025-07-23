// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.xml.XMLSplitter;
import com.boomi.util.StringUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class XMLUtilsTest {

    @Test
    void parseQuietly() throws IOException {
        String content = "<xml><field>value</field></xml>";
        InputStream stream = Mockito.spy(new ByteArrayInputStream(content.getBytes(StringUtil.UTF8_CHARSET)));

        Document document = XMLUtils.parseQuietly(stream);

        Assertions.assertNotNull(document);
        Mockito.verify(stream, Mockito.atLeast(1)).close();
    }

    @Test
    void closeSplitterQuietly() {
        XMLSplitter splitter = Mockito.mock(XMLSplitter.class);
        Mockito.doThrow(new ConnectorException("error!")).when(splitter).close();

        XMLUtils.closeSplitterQuietly(splitter);

        Mockito.verify(splitter, Mockito.times(1)).close();
    }

    @Test
    void closeSplitterQuietlyWithNullReference() {
        XMLUtils.closeSplitterQuietly(null);
    }

    @Test
    void getValueSafely() throws IOException {
        String content = "<xml><field>value</field></xml>";
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return new InputStreamEntity(new ByteArrayInputStream(content.getBytes(StringUtil.UTF8_CHARSET)),
                        ContentType.APPLICATION_XML);
            }
        };
        response = Mockito.spy(response);

        String result = XMLUtils.getValueSafely(response, "field");

        Assertions.assertEquals("value", result);
        Mockito.verify(response, Mockito.atLeast(1)).close();
    }

    @Test
    void getValueSafelyReturnsNullWhenNotFound() throws IOException {
        String content = "<xml><field>value</field></xml>";
        ClassicHttpResponse response = new BasicClassicHttpResponse(200) {
            @Override
            public HttpEntity getEntity() {
                return new InputStreamEntity(new ByteArrayInputStream(content.getBytes(StringUtil.UTF8_CHARSET)),
                        ContentType.APPLICATION_XML);
            }
        };
        response = Mockito.spy(response);

        String result = XMLUtils.getValueSafely(response, "field2");

        Assertions.assertNull(result);
        Mockito.verify(response, Mockito.atLeast(1)).close();
    }
}
