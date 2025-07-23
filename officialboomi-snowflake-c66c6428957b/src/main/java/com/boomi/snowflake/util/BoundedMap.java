// Copyright (c) 2025 Boomi, LP

package com.boomi.snowflake.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A BoundedMap is a specialized implementation of a {@link LinkedHashMap}
 * that limits the number of entries it can hold. When the number of entries
 * exceeds the specified maximum, the oldest entry (based on access order)
 * is automatically removed.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 */
public class BoundedMap<K, V> extends LinkedHashMap<K, V> {
    private final int _maxEntries;

    private static final long serialVersionUID = 1L;
    private static final float FACTOR = 0.75f;

    /**
     * Constructs a new BoundedMap with the specified maximum number of entries.
     * The map will use access-order for iteration, meaning the most recently
     * accessed entries will appear at the end of the iteration order.
     *
     * @param maxEntries the maximum number of entries the map can hold
     * @throws IllegalArgumentException if maxEntries is less than or equal to zero
     */
    public BoundedMap(int maxEntries) {
        // 'true' makes it access-order
        super(maxEntries, FACTOR, true);
        if (maxEntries <= 0) {
            throw new IllegalArgumentException("maxEntries must be greater than 0");
        }
        this._maxEntries = maxEntries;
    }

    /**
     * Determines if the eldest entry should be removed when a new entry is added.
     * This method is called after a put or putAll operation to enforce the size
     * constraint of the map.
     *
     * @param eldest the eldest entry in the map
     * @return {@code true} if the size of the map exceeds the maximum number of entries;
     *         {@code false} otherwise
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > _maxEntries;
    }
}
