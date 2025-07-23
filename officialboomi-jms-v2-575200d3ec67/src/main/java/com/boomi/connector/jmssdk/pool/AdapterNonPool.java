// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;
import com.boomi.util.IOUtil;

import org.apache.commons.pool.PoolableObjectFactory;

/**
 * Simple implementation of {@link AdapterPool} without Pooling capabilities.
 * <p>
 * When invoking {@link #createAdapter()}, this class always returns a new instance. When invoking {@link
 * #releaseAdapter(GenericJndiBaseAdapter)}, the adapter is always disposed.
 */
public class AdapterNonPool implements AdapterPool {

    private final PoolableObjectFactory<GenericJndiBaseAdapter> _factory;

    AdapterNonPool(PoolableObjectFactory<GenericJndiBaseAdapter> factory) {
        _factory = factory;
    }

    @Override
    public GenericJndiBaseAdapter createAdapter() {
        try {
            return _factory.makeObject();
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
    }

    @Override
    public void releaseAdapter(GenericJndiBaseAdapter adapter) {
        IOUtil.closeQuietly(adapter);
    }
}
