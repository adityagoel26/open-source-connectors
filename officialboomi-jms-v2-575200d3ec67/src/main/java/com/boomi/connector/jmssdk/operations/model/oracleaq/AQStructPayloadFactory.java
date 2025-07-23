// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.model.oracleaq;

import oracle.sql.Datum;
import oracle.sql.ORAData;
import oracle.sql.ORADataFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Struct;

/**
 * ORADataFactory implementation used to dequeue from Oracle AQ queues.
 */
public class AQStructPayloadFactory implements ORAData, ORADataFactory {

    private Struct _struct;
    private Connection _connection;

    /**
     * Default constructor required by AQ JMS implementation
     */
    public AQStructPayloadFactory() {
        // required
    }

    private AQStructPayloadFactory(Struct struct) {
        this._struct = struct;
    }

    /*
     * (non-Javadoc)
     *
     * @see oracle.sql.ORADataFactory#create(oracle.sql.Datum, int)
     */
    @Override
    public ORAData create(Datum datum, int sqlType) throws SQLException {
        return new AQStructPayloadFactory((Struct) datum);
    }

    /*
     * (non-Javadoc)
     *
     * @see oracle.sql.ORAData#toDatum(java.sql.Connection)
     */
    @Override
    public Datum toDatum(Connection connection) throws SQLException {
        this._connection = connection;
        return (Datum) _struct;
    }

    /**
     * @return the struct - Struct representation of the message payload
     */
    public Struct getStruct() {
        return _struct;
    }

    /**
     * @return the connection - active connection to oracle
     */
    public Connection getConnection() {
        return _connection;
    }
}
