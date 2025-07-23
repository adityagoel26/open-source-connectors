// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStruct;
import oracle.jdbc.OracleTypeMetaData.Struct;

import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * {@link AQStructMetaDataFactory} implementation for recent versions of oracle.
 *
 * @see BaseStructMetaDataFactory
 */
public class CurrentStructMetaDataFactory extends BaseStructMetaDataFactory {

    /**
     * Creates a new instance with the specified connection
     *
     * @param conn
     */
    public CurrentStructMetaDataFactory(Connection conn) {
        super(conn);
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.browse.StructMetaData#getStructMetaData()
     */
    @Override
    protected ResultSetMetaData getOracleMetaData(String typeName) throws SQLException {
        OracleStruct s = (OracleStruct) _conn.createStruct(typeName, null);
        return ((Struct) s.getOracleMetaData()).getMetaData();
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
        return ((Struct) ((OracleStruct) struct).getOracleMetaData()).getMetaData();
    }

    /*
     * (non-Javadoc)
     *
     * @see com.boomi.connector.jms.metadata.OracleStructWrapperFactory#
     * getArrayBaseType(java.lang.String)
     */
    @Override
    protected AQStructMetaData getBaseArrayMetaData(String typeName) throws SQLException {
        Array array = ((OracleConnection) _conn).createOracleArray(typeName, null);
        return new AQSimpleStruct(typeName, array.getBaseTypeName(), array.getBaseType());
    }
}
