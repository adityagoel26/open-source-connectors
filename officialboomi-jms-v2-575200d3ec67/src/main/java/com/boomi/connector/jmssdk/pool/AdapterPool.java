// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.jmssdk.client.GenericJndiBaseAdapter;

/**
 * An adapter pool can be used to obtain a {@link GenericJndiBaseAdapter}. After finishing using it, the caller must
 * return the adapter to the pool by invoking {@link #releaseAdapter(GenericJndiBaseAdapter)}
 */
public interface AdapterPool {

    /**
     * Creates a new adapter or return an existing one from the pool
     *
     * @return an adapter
     */
    GenericJndiBaseAdapter createAdapter();

    /**
     * Return the given adapter to the pool, or dispose it if its no longer needed.
     *
     * @param adapter the adapter to be released
     */
    void releaseAdapter(GenericJndiBaseAdapter adapter);
}
