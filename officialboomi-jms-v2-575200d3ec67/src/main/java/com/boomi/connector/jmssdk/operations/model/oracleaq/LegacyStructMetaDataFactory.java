// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Legacy {@link AQStructMetaDataFactory} implementation. This behavior was deprecated in oracle version 12 but is
 * required for older versions.
 *
 * @see BaseStructMetaDataFactory
 */
@SuppressWarnings("deprecation")
public class LegacyStructMetaDataFactory extends BaseStructMetaDataFactory {

    /**
     * Creates a new instance with the specified connection
     *
     * @param conn
     */
    public LegacyStructMetaDataFactory(Connection conn) {
        super(conn);
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.boomi.connector.jms.browse.StructMetaData#getStructMetaData(java.lang
     * .String)
     */
    @Override
    protected ResultSetMetaData getOracleMetaData(final String typeName) throws SQLException {
        return StructDescriptor.createDescriptor(typeName, _conn).getMetaData();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.boomi.connector.jms.browse.StructMetaData#getStructMetaData(java.sql.
     * Struct)
     */
    @Override
    protected ResultSetMetaData getOracleMetaData(java.sql.Struct struct) throws SQLException {
        return ((STRUCT) struct).getDescriptor().getMetaData();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.OracleStructWrapperFactory#
     * getArrayBaseType(java.lang.String)
     */
    @Override
    protected AQStructMetaData getBaseArrayMetaData(String typeName) throws SQLException {
        ArrayDescriptor desc = ArrayDescriptor.createDescriptor(typeName, _conn);
        return new AQSimpleStruct(typeName, desc.getBaseName(), desc.getBaseType());
    }
}
