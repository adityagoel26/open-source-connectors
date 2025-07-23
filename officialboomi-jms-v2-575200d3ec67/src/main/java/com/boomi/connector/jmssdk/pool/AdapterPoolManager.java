// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.pool;

import com.boomi.connector.jmssdk.JMSConnection;
import com.boomi.connector.jmssdk.client.settings.AdapterSettings;
import com.boomi.util.ExecutorUtil;
import com.boomi.util.LogUtil;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Singleton class that can be used to create or retrieve instances of {@link AdapterPool}.
 * <p>
 * A Map of active pools is held by this class. Periodically, every 30 minutes, the map is traverse evicting the expired
 * pools.
 */
public final class AdapterPoolManager {

    private static final Logger LOG = LogUtil.getLogger(AdapterPoolManager.class);

    private static final Object LOCK = new Object();
    private static final ConcurrentMap<AdapterSettings, AdapterPoolImpl> ACTIVE_POOLS = new ConcurrentHashMap<>();

    private static final int SHUTDOWN_INTERVAL_MINUTES = 30;
    private static final ScheduledExecutorService SHUTDOWN_SERVICE = ExecutorUtil.newScheduler(
            "JMS Adapter Pool Shutdown Service");

    static {
        Runnable evictor = () -> {
            for (Map.Entry<AdapterSettings, AdapterPoolImpl> poolAndConfig : ACTIVE_POOLS.entrySet()) {
                AdapterPoolImpl pool = poolAndConfig.getValue();
                synchronized (pool) {
                    try {
                        if (pool.isExpired(System.currentTimeMillis()) && !pool.isClosed()) {
                            pool.close();
                        }

                        if (pool.isClosed()) {
                            ACTIVE_POOLS.remove(poolAndConfig.getKey(), pool);
                        }
                    } catch (Exception e) {
                        LOG.log(Level.SEVERE, "unable to close jms adapter pool", e);
                    }
                }
            }
        };

        SHUTDOWN_SERVICE.scheduleAtFixedRate(evictor, SHUTDOWN_INTERVAL_MINUTES, SHUTDOWN_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    private AdapterPoolManager() {
    }

    /**
     * Creates an {@link AdapterPool} using the settings from the given JMSConnection. The concrete implementation of
     * the pool varies depending on connection pool being enabled or not in the given {@link JMSConnection}.
     * <p>
     * When connection pool is enabled, this method may return an already created pool or a new one if there is not one
     * associated with the given {@link JMSConnection}.
     *
     * @param connection containing the connection settings
     * @return an {@link AdapterPool}
     */
    public static AdapterPool getPool(JMSConnection<?> connection) {
        AdapterSettings settings = connection.getAdapterSettings();

        if (!settings.isPoolEnabled()) {
            return new AdapterNonPool(new AdapterFactory(settings));
        }

        AdapterPoolImpl pool = ACTIVE_POOLS.get(settings);
        if (pool != null) {
            return pool;
        }

        // if there isn't a pool created for those settings, sync and check again
        // before actually creating it
        synchronized (LOCK) {
            pool = ACTIVE_POOLS.get(settings);
            if (pool != null) {
                return pool;
            }

            pool = new AdapterPoolImpl(settings, new AdapterFactory(settings));
            ACTIVE_POOLS.put(settings, pool);
        }

        return pool;
    }
}
