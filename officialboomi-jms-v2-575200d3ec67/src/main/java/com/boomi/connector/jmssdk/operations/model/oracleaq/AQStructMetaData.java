// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleTypes;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.Timestamp;

/**
 * This class represents the metadata for an Oracle {@link Struct} instance. This API is meant to simplify interactions
 * with the metadata which would normally involve a {@link ResultSetMetaData} instance.
 */
public interface AQStructMetaData extends Iterable<AQStructMetaData> {

    /**
     * This method returns the name of the attribute
     *
     * @return the name
     */
    String getName();

    /**
     * This method returns the name of the type
     *
     * @return the type name
     */
    String getTypeName();

    /**
     * This method returns an integer value representing the oracle type as defined in {@link OracleTypes}
     *
     * @return the type
     */
    int getType();

    /**
     * This method converts the instance to a json document
     *
     * @return json node representing the object
     */
    JsonNode toJson();

    /**
     * This method returns the count of attributes for the struct. The value represent the number of nested
     * {@link AQStructMetaData} instances that can be iterated over. Simple attributes have metadata but do not have
     * nested attributes.
     *
     * @return the number of attributes, 0 if the struct has no attributes
     */
    int getCount();

    /**
     * This method returns the nested attribute at the specified index. An exception will be thrown if the index does
     * not correspond to an attribute.
     *
     * @param i the index
     * @return the {@link AQStructMetaData} instance at the specified index
     */
    AQStructMetaData get(int i);

    /**
     * This method determines if this is the metadata for an Array
     *
     * @return true if {@link #getType()} returns {@link OracleTypes#ARRAY}, false otherwise
     */
    boolean isArray();

    /**
     * This method determines if this is the metadata for a Struct
     *
     * @return true if {@link #getType()} returns {@link OracleTypes#STRUCT}, false otherwise
     */
    boolean isStruct();

    /**
     * This method determines if this is the metadata for a a complex struct object (i.e., an array or a struct)
     *
     * @return true if this s a complex object, false otherwise
     */
    boolean isComplexType();

    /**
     * This method determines if the represents a date/time data type. Specifically, an oracle type that should be
     * represented as java {@link Timestamp} instance.
     *
     * @return true if the type is a datetime type, false otherwise
     */
    boolean isDateTimeType();
}
