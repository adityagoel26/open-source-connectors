// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.testutil;

import com.boomi.util.ClassUtil;
import com.boomi.util.DOMUtil;
import com.boomi.util.IOUtil;
import com.boomi.util.StreamUtil;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

public class SFRestTestUtil {

    private SFRestTestUtil() {
    }

    public static InputStream getContent(String resource) throws IOException {
        InputStream content = null;
        try {
            content = ClassUtil.getResourceAsStream(resource);
            return StreamUtil.tempCopy(content);
        } finally {
            IOUtil.closeQuietly(content);
        }
    }

    public static String toString(Node inputData) throws TransformerException {
        DOMSource domSource = new DOMSource(inputData);
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);

        TransformerFactory tf = TransformerFactory.newInstance();
        tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        tf.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
        Transformer transformer = tf.newTransformer();

        transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
        transformer.transform(domSource, result);
        return writer.toString();
    }

    public static String getDocumentFromResource(String resourceName) throws Exception {
        InputStream inputStream = SFRestTestUtil.getContent(resourceName);
        Document document = DOMUtil.newDocumentBuilder().parse(inputStream);
        return SFRestTestUtil.toString(document);
    }

    /**
     * Reads and parse an XML InputStream into XMLValueSafeExtractor and returns the content of a target
     * elementName.<br> Returns empty string if the element was not found.<br> Closes the inputStream
     *
     * @return string content of the target element
     * @throws IOException  when fails to parse XML DOM content
     * @throws SAXException when fails to parse XML DOM content
     */
    public static String getElementFromStream(InputStream stream, String elementName) throws Exception {
        try {
            Document document = DOMUtil.newDocumentBuilder().parse(stream);
            return getElementFromDocument(document, elementName);
        } finally {
            IOUtil.closeQuietly(stream);
        }
    }

    /**
     * Returns the content of a target elementName from a document, returns empty string if the element was not found
     *
     * @return string content of the target element
     */
    public static String getElementFromDocument(Document document, String elementName) {
        NodeList items = document.getElementsByTagName(elementName);
        if (items.getLength() == 0) {
            return "";
        }
        return items.item(0).getTextContent();
    }
}
