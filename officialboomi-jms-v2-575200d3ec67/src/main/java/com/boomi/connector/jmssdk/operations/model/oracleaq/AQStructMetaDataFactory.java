// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import java.sql.SQLException;
import java.sql.Struct;

/**
 * Interface to provide uniform access to struct metadata across different versions of oracle
 */
public interface AQStructMetaDataFactory {

    /**
     * Get the metadata for the specified object type
     *
     * @param name     the name of the struct
     * @param typeName the type name of the struct
     * @return {@link AQStructMetaData} for the struct
     * @throws SQLException
     */
    AQStructMetaData getTypeMetaData(String name, String typeName) throws SQLException;

    /**
     * Get the metadata for the specified struct instance
     *
     * @param name   the name of the struct
     * @param struct the struct
     * @return {@link AQStructMetaData} for the struct
     * @throws SQLException
     */
    AQStructMetaData getStructMetaData(String name, Struct struct) throws SQLException;
}
