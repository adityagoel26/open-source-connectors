// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.cache;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.util.ConnectorCache;
import com.boomi.connector.util.ConnectorCacheFactory;

import java.sql.Connection;

/**
 * This class is used to store the {@link Connection} in cache.
 */
public class TransactionCache extends ConnectorCache<TransactionCacheKey> {

    private Connection  _connection;

    public TransactionCache(TransactionCacheKey key, Connection connection) {
        super(key);
        _connection = connection;
    }

    /**
     * Returns the implementation of {@link ConnectorCacheFactory}
     * @param connection
     * @return
     */
    public static ConnectorCacheFactory<TransactionCacheKey, TransactionCache, ConnectorContext>
    getConnectionFactory(
            Connection connection) {
        // ConnectorCacheFactory#createCache implementation
        return (cacheKey, context) -> new TransactionCache(cacheKey, connection);
    }

    public Connection getConnection() {
        return _connection;
    }

    public void setConnection(Connection connection) {
        this._connection = connection;
    }
}
