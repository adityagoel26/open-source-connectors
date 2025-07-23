// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.testutil.NoLoggingTest;
import com.boomi.util.StreamUtil;
import com.boomi.util.StringUtil;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TextXMLPayloadValidatorTest extends NoLoggingTest {

    @Test
    public void isXMLTest() throws IOException {
        InputStream xmlStream = new ByteArrayInputStream("<xml>some node</xml>".getBytes(StringUtil.UTF8_CHARSET));
        InputStream nonXmlStream = new ByteArrayInputStream("some text".getBytes(StringUtil.UTF8_CHARSET));

        boolean xmlResult = TextXMLPayloadValidator.hasXMLContent(xmlStream);
        boolean nonXmlResult = TextXMLPayloadValidator.hasXMLContent(nonXmlStream);

        assertTrue("the provided stream holds an xml document", xmlResult);
        assertFalse("the provided stream does not hold an xml document", nonXmlResult);
    }

    @Test
    public void validateXmlTest() {
        InputStream xmlStream = new ByteArrayInputStream("<xml>some node</xml>".getBytes(StringUtil.UTF8_CHARSET));

        TextXMLPayloadValidator.assertXMLContent(xmlStream);
    }

    @Test(expected = ConnectorException.class)
    public void validateNonXmlTest() throws IOException {
        InputStream nonXmlStream = new ByteArrayInputStream("some text".getBytes(StringUtil.UTF8_CHARSET));

        TextXMLPayloadValidator.assertXMLContent(nonXmlStream);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void isXMLShouldCallResetOnExceptionTest() throws IOException {
        InputStream mockedStream = mock(InputStream.class);
        when(mockedStream.read()).thenThrow(IOException.class);

        boolean nonXmlResult = TextXMLPayloadValidator.hasXMLContent(mockedStream);

        assertFalse("the provided stream does not hold an xml document", nonXmlResult);
        verify(mockedStream, times(1)).reset();
    }

    @Test(expected = ConnectorException.class)
    public void validateXmlShouldCallResetTest() throws IOException {
        String xmlPayload = "<xml>this is the xml payload</xml>";
        ByteArrayInputStream xmlStream = new ByteArrayInputStream(xmlPayload.getBytes(StringUtil.UTF8_CHARSET));
        String nonXmlPayload = "this is the non xml payload";
        ByteArrayInputStream nonXmlStream = new ByteArrayInputStream(nonXmlPayload.getBytes(StringUtil.UTF8_CHARSET));

        try {
            TextXMLPayloadValidator.assertXMLContent(xmlStream);
            TextXMLPayloadValidator.assertXMLContent(nonXmlStream);
        } finally {
            assertThat(StreamUtil.toString(xmlStream, StringUtil.UTF8_CHARSET), is(xmlPayload));
            assertThat(StreamUtil.toString(nonXmlStream, StringUtil.UTF8_CHARSET), is(nonXmlPayload));
        }
    }
}
