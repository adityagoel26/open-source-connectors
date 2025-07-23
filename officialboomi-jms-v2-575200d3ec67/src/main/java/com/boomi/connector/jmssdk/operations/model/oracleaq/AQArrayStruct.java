// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleTypes;

import java.util.Arrays;

/**
 * {@link AQStructMetaData} implementation for array metadata. Instances of this class will always have
 * {@link OracleTypes#ARRAY} and will have exactly one nested attribute. The nested attribute describes the metadata of
 * objects that can appear in the array.
 */
public class AQArrayStruct extends AQComplexStruct {

    /**
     * Creates new instance
     *
     * @param name          the name of the array
     * @param type          the name of the type of the array
     * @param arrayMetaData {@link AQStructMetaData} instance describing the the type of elements that can appear in the
     *                      array
     */
    public AQArrayStruct(String name, String type, AQStructMetaData arrayMetaData) {
        super(name, type, OracleTypes.ARRAY, Arrays.asList(arrayMetaData));
    }
}
