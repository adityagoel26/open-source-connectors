// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.util;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.util.xml.XMLSplitter;
import com.boomi.salesforce.rest.data.XMLValueSafeExtractor;
import com.boomi.util.DOMUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.LogUtil;

import org.apache.hc.core5.http.ClassicHttpResponse;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Designed to parse Size Limited inputs, and small sized Metadata responses.<br> Closes each stream after parsing.
 */
public class XMLUtils {
    private static final Logger LOG = LogUtil.getLogger(XMLUtils.class);

    private XMLUtils() {
    }

    /**
     * Reads and parse an XML ClassicHttpResponse into XMLValueSafeExtractor and returns the content of a target
     * elementName.<br> Returns null if the element was not found.<br> Closes the ClassicHttpResponse
     *
     * @return string content of the target element
     * @throws XMLStreamException when fails to parse XML content
     */
    public static String getValueSafely(ClassicHttpResponse response, String elementName) {
        XMLValueSafeExtractor extractor = null;
        try {
            extractor = new XMLValueSafeExtractor(response, Collections.singletonList(elementName));
            return extractor.getValue(elementName);
        } catch (XMLStreamException e) {
            throw new ConnectorException("[Errors occurred while parsing XML] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(extractor);
        }
    }

    /**
     * Parses the size limited InputStream into an XML Document.<br> Closes the inputStream
     *
     * @return XML Document
     * @throws ConnectorException if failed to parse the stream into Document
     */
    public static Document parseQuietly(InputStream stream) throws ConnectorException {
        try {
            return DOMUtil.newDocumentBuilder().parse(stream);
        } catch (SAXException | IOException e) {
            throw new ConnectorException("[Errors occurred while parsing XML] " + e.getMessage(), e);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    /**
     * For some reason this is better than IOUtil.closeQuietly, the later stuck unresponsive for some cases (i.e output
     * more than 100 documents in test mode)
     */
    public static void closeSplitterQuietly(XMLSplitter splitter) {
        try {
            splitter.close();
        } catch (Exception e) {
            LOG.log(Level.INFO, e, e::getMessage);
        }
    }

    /**
     * Get an instance of {@link XMLInputFactory} with the appropriate security configuration
     *
     * @return the XMLInputFactory instance
     */
    public static XMLInputFactory getXmlInputFactory() {
        XMLInputFactory factory = XMLInputFactory.newInstance();

        // Splits large content data
        factory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.FALSE);

        // set security properties
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);

        return factory;
    }
}
