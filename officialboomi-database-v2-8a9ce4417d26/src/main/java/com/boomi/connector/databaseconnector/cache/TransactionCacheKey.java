// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.databaseconnector.cache;

import com.boomi.connector.api.PropertyMap;
import com.boomi.util.HashCodeBuilder;
import com.boomi.util.ObjectUtil;

/**
 * Key for the {@link com.boomi.connector.util.ConnectorCache}
 */
public class TransactionCacheKey {

    public static final String TRANSACTION_ID_PREFIX = "TransactionId(";
    private final String _id;
    private final PropertyMap _connectionProperties;

    public TransactionCacheKey(String id, PropertyMap connectionProperties) {
        _id = id;
        _connectionProperties = connectionProperties;
    }

    /**
     * Compares if two CacheKeys are equal.
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TransactionCacheKey other = (TransactionCacheKey) o;
        return (ObjectUtil.equals(other._id, _id) && ObjectUtil.equals(other._connectionProperties,
                _connectionProperties));
    }

    /**
     * Calculates hash from _topLevelExecutionId and _propertyMap
     * @return
     */
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(_id).append(_connectionProperties).toHashCode();
    }

    /**
     * Returns Transaction Id
     * @return
     */
    @Override
    public String toString() {
        return TRANSACTION_ID_PREFIX + hashCode() + ")";
    }
}
