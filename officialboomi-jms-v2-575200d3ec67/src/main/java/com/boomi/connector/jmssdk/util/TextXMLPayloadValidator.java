// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.util.LogUtil;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class TextXMLPayloadValidator {

    private static final Logger LOG = LogUtil.getLogger(TextXMLPayloadValidator.class);
    private static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory.newInstance();

    private TextXMLPayloadValidator() {
    }

    /**
     * Validate that the context of the given {@link InputStream} is an XML document, throwing a {@link
     * ConnectorException} when it is not.
     * <p>
     * After performing the validation, {@link InputStream#reset()} is invoked.
     *
     * @param payload to be validated
     * @return the same InputStream after being reset
     * @throws ConnectorException when the Payload does not contain an XML document
     */
    public static InputStream assertXMLContent(InputStream payload) {
        try {
            if (containsXML(payload)) {
                return payload;
            }

            throw new ConnectorException("invalid xml document");
        } catch (XMLStreamException | IOException e) {
            throw new ConnectorException("cannot parse xml document", e);
        }
    }

    /**
     * Verify if the given {@link InputStream} contains an XML document or not.
     * <p>
     * After performing the validation, {@link InputStream#reset()} is invoked.
     *
     * @param payload to be validated
     * @return {@code true} if the {@link InputStream} contains an XML document, {@code false} otherwise
     */
    public static boolean hasXMLContent(InputStream payload) {
        try {
            return containsXML(payload);
        } catch (XMLStreamException | IOException e) {
            LOG.log(Level.INFO, "cannot parse text message as XML", e);
            return false;
        }
    }

    private static boolean containsXML(InputStream payload) throws XMLStreamException, IOException {
        try {
            XMLStreamReader reader = XML_INPUT_FACTORY.createXMLStreamReader(new StreamSource(payload));
            while (reader.hasNext()) {
                if (reader.next() == XMLStreamConstants.START_ELEMENT) {
                    return true;
                }
            }
            return false;
        } finally {
            payload.reset();
        }
    }
}
