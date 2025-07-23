// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.operations.model.TargetDestination;
import com.boomi.util.DBUtil;
import com.boomi.util.SchemaBuilder;
import com.boomi.util.SchemaUtil;
import com.boomi.util.XMLUtil;

import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.io.InputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public final class AQObjectFactory {

    private static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory.newInstance();

    static {
        XML_INPUT_FACTORY.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
    }

    private final AQDataConverter _convert = new AQDataConverter();
    private final Connection _conn;

    private AQObjectFactory(Connection conn) {
        if (conn == null) {
            throw new ConnectorException("null connection");
        }
        this._conn = conn;
    }

    /**
     * Create factory instance
     *
     * @param conn active java.sql.Connection instance
     * @return factory instance
     */
    public static AQObjectFactory instance(Connection conn) {
        return new AQObjectFactory(conn);
    }

    /**
     * Create an xml representation of the specified Struct.
     * <p>
     * Column names that do not conform to xml standards will be formatted by replacing non-conforming characters with
     * an underscore. If formatting results in duplicate elements at the same level of the document, subsequent elements
     * will be include a numeric suffix.
     * <p>
     * Since the root element corresponds to a queue or topic name, the queue: and topic: prefixes may be present but
     * will be removed in the resulting xml.
     *
     * @param elementName the root element of the xml representation
     * @param struct
     * @return document instance of the xml representation
     */
    public Document createDocument(AQStructMetaData structMetaData, Struct struct) {
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document doc = builder.newDocument();
            doc.appendChild(createElement(structMetaData, struct, doc));
            return doc;
        } catch (Exception e) {
            throw new ConnectorException("unable to create document for: " + structMetaData.toJson(), e);
        }
    }

    /**
     * Recursive method to create XML documents for the specified {@link Struct} instance. The document will use
     * {@link AQComplexStruct#getName()} as the root element. Simple attributes will be added as single elements. Arrays
     * will be added as complex elements with an {@value #ARRAY_ITEM} element for each item in the array. The method
     * will be called recursively for nested Struct instances.
     * <p>
     * An {@link AQDataConverter} instance will be used to convert all data values from the struct representation to the
     * xml representation.
     */
    private Element createElement(AQStructMetaData metaData, Struct struct, Document doc) throws Exception {
        Element root = doc.createElement(metaData.getName());
        if (struct != null) {
            for (int i = 0; i < metaData.getCount(); i++) {
                root.appendChild(createElementValue(metaData.get(i), struct.getAttributes()[i], doc));
            }
        }
        return root;
    }

    private Element createElementValue(AQStructMetaData metaData, Object attributeValue, Document doc)
            throws Exception {
        Element elementValue;
        if (metaData.isStruct()) {
            elementValue = createElement(metaData, (Struct) attributeValue, doc);
        } else if (metaData.isArray()) {
            elementValue = createArray(metaData, (Array) attributeValue, doc);
        } else {
            elementValue = doc.createElement(metaData.getName());
            elementValue.setTextContent(_convert.fromObject(attributeValue, metaData));
        }
        return elementValue;
    }

    private Element createArray(AQStructMetaData metaData, Array attributeValue, Document doc) throws Exception {
        Element array = doc.createElement(metaData.getName());
        AQStructMetaData arrayMetaData = metaData.iterator().next();

        for (Object itemContent : (Object[]) attributeValue.getArray()) {
            array.appendChild(createElementValue(arrayMetaData, itemContent, doc));
        }

        return array;
    }

    /**
     * Create an xml schema representing the specified queue
     * <p>
     * Column names that do not conform to xml standards will be formatted by replacing non-conforming characters with
     * an underscore. If formatting results in duplicate elements at the same level of the document, subsequent elements
     * will be include a numeric suffix.
     *
     * @param metaData {@linkplain AQStructMetaData} representing the queue type definition
     * @return document element instance representing the schema
     */
    public static Element createProfileSchema(AQStructMetaData metaData) {
        SchemaBuilder builder = new SchemaBuilder().appendSchemaElement(metaData.getName(), metaData.getTypeName())
                .append(SchemaUtil.COMPLEX_TYPE).appendSequence();

        for (AQStructMetaData subMeta : metaData) {
            if (subMeta.isComplexType()) {
                builder.appendImported(createProfileSchema(subMeta).getFirstChild());
            } else {
                builder.appendSchemaElement(subMeta.getName(), getSimpleXsType(subMeta));
            }

            if (metaData.isArray()) {
                builder.setOccursOptionalMulti();
            } else {
                builder.setOccursOptional();
            }

            builder.toParent();
        }

        return builder.getDocumentElement();
    }

    private static String getSimpleXsType(AQStructMetaData metaData) {
        return metaData.isDateTimeType() ? SchemaUtil.TYPE_DATETIME : SchemaUtil.TYPE_STRING;
    }

    /**
     * Create a Struct instance based on the xml input stream.
     * <p>
     * The struct is intended to represent a targetDestination message so the xml must conform to the generated schema
     * for the target targetDestination
     *
     * @param xml
     * @return Struct representation of the xml
     */
    public Struct createStruct(InputStream xml, AQStructMetaDataFactory metaDataFactory,
            TargetDestination targetDestination) {
        XMLStreamReader reader = null;
        try {
            reader = XML_INPUT_FACTORY.createFilteredReader(XML_INPUT_FACTORY.createXMLStreamReader(xml),
                    new WhiteSpaceFilter());
            while (reader.hasNext() && !reader.isStartElement()) {
                reader.next();
            }
            String name = reader.getLocalName();
            String type = targetDestination.getDataType();
            reader.next();
            return readStruct(reader, metaDataFactory.getTypeMetaData(name, type));
        } catch (Exception e) {
            throw new ConnectorException("unable to create object message from input xml", e);
        } finally {
            XMLUtil.closeQuietly(reader);
        }
    }

    /**
     * Recursive method to create a {@link Struct} instance from input xml. The struct attributes array is required to
     * contain an entry for all defined fields. A null entry will be added for all attributes that do not have a
     * corresponding element in the xml document.
     */
    private Struct readStruct(XMLStreamReader reader, AQStructMetaData metaData)
            throws XMLStreamException, SQLException, ParseException {

        int index = 0;
        Object[] attributes = new Object[metaData.getCount()];
        do {
            if (reader.isStartElement()) {
                String elementName = readElementName(reader);

                AQStructMetaData current = metaData.get(index);
                while (!elementName.equals(current.getName())) {
                    index++;
                    current = metaData.get(index);
                }

                attributes[index] = readObjectValue(reader, current);
                index++;
            }

            if (reader.hasNext()) {
                reader.next();
            } else {
                throw new ConnectorException("invalid xml structure");
            }
        } while (!(reader.isEndElement() && reader.getLocalName().equals(metaData.getName())));

        return _conn.createStruct(metaData.getTypeName(), attributes);
    }

    private Object[] readArray(XMLStreamReader reader, AQStructMetaData arrayMetaData)
            throws XMLStreamException, SQLException, ParseException {
        List<Object> arrayValues = new ArrayList<>();
        while (reader.isStartElement() && arrayMetaData.getName().equals(reader.getLocalName())) {
            reader.next();

            arrayValues.add(readObjectValue(reader, arrayMetaData));

            reader.next();
        }
        return arrayValues.toArray();
    }

    private Object readObjectValue(XMLStreamReader reader, AQStructMetaData metaData)
            throws XMLStreamException, SQLException, ParseException {
        if (metaData.isStruct()) {
            return readStruct(reader, metaData);
        } else if (metaData.isArray()) {
            return readArray(reader, metaData.iterator().next());
        } else {
            return _convert.fromString(readCharacters(reader), metaData, _conn);
        }
    }

    private static String readCharacters(XMLStreamReader reader) throws XMLStreamException {
        String characters = null;
        if (reader.isCharacters()) {
            characters = reader.getText();
            reader.next();
        }
        return characters;
    }

    private static String readElementName(XMLStreamReader reader) throws XMLStreamException {
        String elementName = reader.getLocalName();
        reader.next();
        return elementName;
    }

    /**
     * Close active jdbc connections
     */
    public void close() {
        DBUtil.closeQuietly(_conn);
    }



    private static class WhiteSpaceFilter implements StreamFilter {

        @Override
        public boolean accept(XMLStreamReader r) {
            return !r.isWhiteSpace();
        }
    }
}
