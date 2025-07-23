// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.LogUtil;
import com.boomi.util.ObjectUtil;
import com.boomi.util.TimeIntervalUnit;

import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class leverages {@link GenericObjectPool} to implement a Pool of {@link GenericJndiBaseAdapter}.
 * <p>
 * When invoking {@link #createAdapter()}, this class may return a new adapter or an existing one already present in the
 * pool. When invoking {@link #releaseAdapter(GenericJndiBaseAdapter)}, the adapter is validated and returned to the
 * pool.
 */
final class AdapterPoolImpl extends GenericObjectPool<GenericJndiBaseAdapter> implements AdapterPool {

    private static final Logger LOG = LogUtil.getLogger(AdapterPoolImpl.class);
    /*
     * How long an idle pool may live after the last connection is returned before it is considered expired.
     */
    private static final long POOL_EXPIRATION_INTERVAL = TimeIntervalUnit.H(6);

    private volatile long _lastAccessTime = System.currentTimeMillis();

    AdapterPoolImpl(AdapterSettings settings, PoolableObjectFactory<GenericJndiBaseAdapter> factory) {
        super(factory);
        setConfig(settings.getPoolConfig());
    }

    @Override
    public GenericJndiBaseAdapter createAdapter() {
        try {
            GenericJndiBaseAdapter adapter = borrowObject();
            _lastAccessTime = System.currentTimeMillis();
            return adapter;
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened getting an adapter from the pool", e);
            ObjectUtil.propagateInterruption(e);
            throw new ConnectorException(e);
        }
    }

    @Override
    public void releaseAdapter(GenericJndiBaseAdapter adapter) {
        try {
            returnObject(adapter);
            _lastAccessTime = System.currentTimeMillis();
        } catch (Exception e) {
            LOG.log(Level.WARNING, "an error happened returning an adapter to the pool", e);
            throw new ConnectorException(e);
        }
    }

    /**
     * This pool is considered expired if it does not hold any active adapter and it has not been used in the last 6
     * hours
     *
     * @param currentTime the current time in millis
     * @return {@code true} if the pool is expired, {@code false} otherwise
     */
    boolean isExpired(long currentTime) {
        return (getNumActive() <= 0) && (POOL_EXPIRATION_INTERVAL < (currentTime - _lastAccessTime));
    }
}
