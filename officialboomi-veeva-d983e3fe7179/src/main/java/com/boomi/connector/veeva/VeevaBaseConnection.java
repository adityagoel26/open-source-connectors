// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva;

import com.boomi.common.rest.RestConnection;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.util.ConnectorCache;

import java.net.URL;
import java.util.Collections;
import java.util.Map;

/**
 * Base interface for Veeva connections, added considering that the first common ancestor is {@link RestConnection}.
 */
public interface VeevaBaseConnection {

    /**
     * Returns the connector context.
     *
     * @return connector context
     */
    ConnectorContext getContext();

    /**
     * Returns the Veeva Vault API endpoint to interact with, including the API version in the path.
     *
     * @return string, the Veeva Vault API endpoint
     */

    URL getUrl();

    /**
     * Returns a Session ID from the connector cache.
     *
     * @return string representing a Veeva Vault session
     */
    String getSessionIdFromCache();

    /**
     * Returns a key used to store Session IDs in the connector cache.
     *
     * @return string representing a key to cache Veeva Vault sessions
     */
    String getSessionCacheKey();

    /**
     * Returns the custom headers for the given Tracked Data.
     *
     * @param data the tracked data to extract the custom headers
     * @return non-null Map containing custom header keys and values
     */
    default Map<String, String> getCustomHeaders(TrackedData data) {
        if (data == null) {
            return Collections.emptyMap();
        }
        return data.getDynamicOperationProperties().getCustomProperties("customHeaders");
    }

    /**
     * Clear the cache instance corresponding to the session cache key representing this connection configuration.
     */
    default void clearCache() {
        ConnectorCache.clearCache(getSessionCacheKey(), getContext());
    }
}
