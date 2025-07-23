// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.PropertyMap;
import com.boomi.util.EqualsBuilder;
import com.boomi.util.HashCodeBuilder;
import com.boomi.util.NumberUtil;

import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Value object holding the configuration related to the Adapter Pool.
 * <p>
 * This class overrides {@link #equals(Object)} & {@link #hashCode()} methods: two instances are considered equal if
 * they shared the same values for all their fields.
 */
public class AdapterPoolSettings {

    private static final int MAX_CONNECTIONS_DEFAULT = 10;
    private static final int MIN_CONNECTIONS_DEFAULT = 0;
    private static final int MAX_IDLE_TIME_DEFAULT = 10;
    private static final int MAX_WAIT_TIME_DEFAULT = 10;

    private static final String INVALID_NUMERIC_PROPERTY_ERROR_FORMAT =
            "invalid value %s for key %s, it cannot be greater than %s or less than 0";

    private final boolean _isEnabled;
    private final int _maximumConnections;
    private final int _minimumConnections;
    private final ExhaustedAction _exhaustedAction;
    private final long _maximumIdleTime;
    private final long _maximumWaitTime;

    private enum ExhaustedAction {
        WAIT_FOR_CONNECTION, IMMEDIATELY_FAIL
    }

    public AdapterPoolSettings(PropertyMap configs) {
        _isEnabled = configs.getBooleanProperty("use_connection_pooling", false);
        _maximumConnections = getIntProp(configs, "pool_maximum_connections", MAX_CONNECTIONS_DEFAULT);
        _minimumConnections = getIntProp(configs, "pool_minimum_connections", MIN_CONNECTIONS_DEFAULT);
        _maximumIdleTime = getIntProp(configs, "pool_maximum_idle_time", MAX_IDLE_TIME_DEFAULT);
        _maximumWaitTime = getIntProp(configs, "pool_maximum_wait_time", MAX_WAIT_TIME_DEFAULT);
        _exhaustedAction = NumberUtil.toEnum(ExhaustedAction.class, configs.getProperty("pool_exhausted_action"),
                ExhaustedAction.WAIT_FOR_CONNECTION);
    }

    private static int getIntProp(PropertyMap props, String key, int defaultValue) {
        Long value = props.getLongProperty(key);
        if (value == null) {
            return defaultValue;
        }

        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new ConnectorException(
                    String.format(INVALID_NUMERIC_PROPERTY_ERROR_FORMAT, value, key, Integer.MAX_VALUE));
        }

        return value.intValue();
    }

    /**
     * Create a {@link org.apache.commons.pool.impl.GenericObjectPool.Config} and fill it with the settings contained by
     * this instance
     *
     * @return a Config object with the settings held by this instance
     */
    public GenericObjectPool.Config toConfig() {
        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.maxActive = _maximumConnections;
        config.minIdle = _minimumConnections;
        config.maxWait = _maximumWaitTime;
        config.softMinEvictableIdleTimeMillis = _maximumIdleTime;

        switch (_exhaustedAction) {
            case IMMEDIATELY_FAIL:
                config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_FAIL;
                break;
            case WAIT_FOR_CONNECTION:
                config.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
                break;
            default:
                throw new UnsupportedOperationException("unknown exhausted action: " + _exhaustedAction);
        }

        config.testOnBorrow = true;
        config.testOnReturn = true;
        return config;
    }

    /**
     * Indicate if Connection Pool is enabled or not.
     *
     * @return {@code true} if connection pool is enabled, {@code false} otherwise.
     */
    public boolean isEnabled() {
        return _isEnabled;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof AdapterPoolSettings)) {
            return false;
        }

        AdapterPoolSettings other = (AdapterPoolSettings) o;
        return new EqualsBuilder().append(_isEnabled, other._isEnabled).append(_maximumConnections,
                other._maximumConnections).append(_minimumConnections, other._minimumConnections).append(
                _maximumIdleTime, other._maximumIdleTime).append(_maximumWaitTime, other._maximumWaitTime).append(
                _exhaustedAction, other._exhaustedAction).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(_isEnabled).append(_maximumConnections).append(_minimumConnections).append(
                _maximumIdleTime).append(_maximumWaitTime).append(_exhaustedAction).hashCode();
    }
}
