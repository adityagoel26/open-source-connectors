// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite.batch;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.salesforce.rest.data.XMLStreamOmittingDeclaration;
import com.boomi.salesforce.rest.util.XMLUtils;
import com.boomi.util.IOUtil;
import com.boomi.util.StringUtil;
import com.boomi.util.XMLUtil;
import com.boomi.util.io.FastByteArrayInputStream;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.transform.TransformerException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SFCompositeCombiner {

    private static final int COMPOSITE_BASE_STREAM_COUNT = 3;
    public static final String ATTR_TYPE = "type";
    public static final String RECORDS_NODE = "records";

    /**
     * Wraps the list of inputs along with the root header and the allOrNone tag in a single InputStream
     *
     * @param requestBatch list of inputs to be wrapped in a batch
     * @param allOrNone    boolean to include allOrNone tag
     * @param sobjectName  the SF Object name
     * @return InputStream includes the whole batch request body
     */
    public InputStream buildCompositeBody(List<ObjectData> requestBatch, boolean allOrNone, String sobjectName) {
        Collection<InputStream> inputs = new ArrayList<>(requestBatch.size() + COMPOSITE_BASE_STREAM_COUNT);
        inputs.add(toInputStream("<result>"));
        if (allOrNone) {
            inputs.add(toInputStream("<allOrNone>True</allOrNone>"));
        }
        for (ObjectData document : requestBatch) {
            inputs.add(new XMLStreamOmittingDeclaration(insertTypeAttribute(document.getData(), sobjectName)));
        }
        inputs.add(toInputStream("</result>"));

        return new SequenceInputStream(Collections.enumeration(inputs));
    }

    /**
     * Inserts a type attribute to the records element of an XML document, if it is missing or blank.
     *
     * @param source       the input stream of the XML document
     * @param objectTypeID the value of the type attribute to be inserted
     * @return a new input stream of the modified XML document
     */
    private static InputStream insertTypeAttribute(InputStream source, String objectTypeID) {
        try {
            Document document = XMLUtils.parseQuietly(source);
            XPath nodeLocator = XPathFactory.newInstance().newXPath();
            Element records = (Element) nodeLocator.evaluate(RECORDS_NODE, document, XPathConstants.NODE);

            if (records != null && StringUtil.isBlank(records.getAttribute(ATTR_TYPE))) {
                records.setAttribute(ATTR_TYPE, objectTypeID);
            }

            return XMLUtil.encodeTempXML(document);
        } catch (XPathExpressionException | IOException | TransformerException e) {
            throw new ConnectorException(e);
        } finally {
            IOUtil.closeQuietly(source);
        }
    }

    public String buildCompositeDelete(List<ObjectIdData> inputBatch) {
        return inputBatch.stream().map(ObjectIdData::getObjectId).collect(Collectors.joining(","));
    }

    /**
     * Converts string to InputStream
     *
     * @param s string to be converted
     * @return InputStream
     */
    private static InputStream toInputStream(String s) {
        return new FastByteArrayInputStream(s.getBytes(StringUtil.UTF8_CHARSET));
    }
}