// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.internal.OracleTypes;
import com.boomi.connector.api.ConnectorException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * {@link AQStructMetaData} implementation for attributes / types that do not have their own attributes.
 */
public class AQSimpleStruct implements AQStructMetaData {

    @SuppressWarnings("deprecation")
    private static final Set<Integer> DATE_TIME_TYPES = new HashSet<>(
            Arrays.asList(OracleTypes.DATE, OracleTypes.TIME, OracleTypes.TIMESTAMP, OracleTypes.TIMESTAMPLTZ,
                    OracleTypes.TIMESTAMPNS, OracleTypes.TIMESTAMPTZ));

    private static final String TYPE = "type";
    private static final String TYPE_NAME = "typeName";
    private static final String NAME = "name";
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String _name;
    private final String _typeName;
    private final int _type;

    /**
     * Creates a new instance
     *
     * @param name     the name of the struct
     * @param typeName the name of the struct type
     * @param type     the type of the struct
     */
    public AQSimpleStruct(String name, String typeName, int type) {
        _name = name;
        _typeName = typeName;
        _type = type;
    }

    /**
     * Creates a new instance from json data representing a previous instance
     *
     * @param json the json string
     */
    public AQSimpleStruct(JsonNode json) {
        try {
            _name = json.get(NAME).asText();
            _typeName = json.get(TYPE_NAME).asText();
            _type = json.get(TYPE).asInt();
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQObjectAttribute#getName()
     */
    @Override
    public String getName() {
        return _name;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQObjectAttribute#getTypeName()
     */
    @Override
    public String getTypeName() {
        return _typeName;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQObjectAttribute#getType()
     */
    @Override
    public int getType() {
        return _type;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQObjectAttribute#toJson()
     */
    @Override
    public JsonNode toJson() {
        ObjectNode json = MAPPER.createObjectNode();
        json.put(NAME, _name);
        json.put(TYPE_NAME, _typeName);
        json.put(TYPE, _type);
        return json;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Iterable#iterator()
     */
    @Override
    public Iterator<AQStructMetaData> iterator() {
        return Collections.<AQStructMetaData>emptyList().iterator();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#getCount()
     */
    @Override
    public int getCount() {
        return 0;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#get(int)
     */
    @Override
    public AQStructMetaData get(int i) {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#isArray()
     */
    @Override
    public boolean isArray() {
        return _type == OracleTypes.ARRAY;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#isStruct()
     */
    @Override
    public boolean isStruct() {
        return _type == OracleTypes.STRUCT;
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#isComplexType()
     */
    @Override
    public boolean isComplexType() {
        return isArray() || isStruct();
    }

    /* (non-Javadoc)
     * @see com.boomi.connector.jms.metadata.AQStructMetaData#isDateTimeType()
     */
    @Override
    public boolean isDateTimeType() {
        return DATE_TIME_TYPES.contains(_type);
    }
}
