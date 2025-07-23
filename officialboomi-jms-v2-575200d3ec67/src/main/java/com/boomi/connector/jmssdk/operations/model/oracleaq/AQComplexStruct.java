// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * {@link AQStructMetaData} implementation for attributes / types that have their own attributes. More specifically,
 * instances of this class contain the metadata for Struct objects and {@link #getType()} will always return
 * {@link OracleTypes#STRUCT}.
 */
public class AQComplexStruct extends AQSimpleStruct {

    private static final String ATTRIBUTES = "attributes";

    private final List<AQStructMetaData> _attributes;

    protected AQComplexStruct(String name, String typeName, int type, List<AQStructMetaData> attributes) {
        super(name, typeName, type);
        _attributes = new ArrayList<>();
        if (attributes != null) {
            _attributes.addAll(attributes);
        }
    }

    /**
     * Creates a new instance with the specified name, type, and attributes.
     *
     * @param name       the name of the struct
     * @param type       the name of the struct type
     * @param attributes list of {@link AQStructMetaData} representing the attributes of the corresponding
     *                   {@link Struct} instance.
     */
    public AQComplexStruct(String name, String type, List<AQStructMetaData> attributes) {
        this(name, type, OracleTypes.STRUCT, attributes);
    }

    /**
     * Creates a new instance with an unknown SQL type name and no attributes
     *
     * @param name the name of the struct
     */
    public AQComplexStruct(String name) {
        this(name, null, Collections.<AQStructMetaData>emptyList());
    }

    /**
     * Creates a new instance from json data representing a previous instance. The resulting instance will represent the
     * full type including any nested attributes.
     *
     * @param json the json data
     */
    public AQComplexStruct(JsonNode json) {
        super(json);
        _attributes = new ArrayList<>();
        if (hasAttributes(json)) {
            for (JsonNode attr : json.get(ATTRIBUTES)) {
                _attributes.add(hasAttributes(attr) ? new AQComplexStruct(attr) : new AQSimpleStruct(attr));
            }
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQSimpleAttribute#toJson()
     */
    @Override
    public JsonNode toJson() {
        ObjectNode json = (ObjectNode) super.toJson();
        ArrayNode attributes = json.putArray(ATTRIBUTES);
        for (AQStructMetaData attr : _attributes) {
            attributes.add(attr.toJson());
        }
        return json;
    }

    /**
     * This method returns the number of attributes in the {@link ResultSetMetaData} instance.
     *
     * @return the number of attributes
     * @throws SQLException
     */
    @Override
    public int getCount() {
        return _attributes.size();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<AQStructMetaData> iterator() {
        return _attributes.iterator();
    }

    /**
     * This method returns the nested {@link AQStructMetaData} at the specified index
     *
     * @param i 0 based index of the attribute
     * @return the attribute
     */
    @Override
    public AQStructMetaData get(int i) {
        return _attributes.get(i);
    }

    private static boolean hasAttributes(JsonNode json) {
        return json.hasNonNull(ATTRIBUTES);
    }
}
