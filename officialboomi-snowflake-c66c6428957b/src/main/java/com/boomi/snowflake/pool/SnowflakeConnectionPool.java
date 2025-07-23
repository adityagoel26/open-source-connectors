// Copyright (c) 2024 Boomi, LP
package com.boomi.snowflake.pool;

import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import com.boomi.util.ExecutorUtil;

import net.snowflake.client.jdbc.internal.apache.commons.codec.digest.DigestUtils;

/**
 * The Class SnowflakeConnectionPool.
 *
 * @author Vanangudi,S
 */
public class SnowflakeConnectionPool {
	
	/** The Constant DEFAULT_EVICTION_INTERVAL_UNITS. */
	private static final TimeUnit DEFAULT_EVICTION_INTERVAL_UNITS = TimeUnit.MINUTES;
	
	/** The ScheduledExecutorService SHUTDOWN_SERVICE . */
	private static final ScheduledExecutorService SHUTDOWN_SERVICE = ExecutorUtil.newScheduler("DatabaseConnector Pool Shutdown Service");

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(SnowflakeConnectionPool.class.getName());

	/** The ConcurrentHashMap DATASOURCE_COLLECTION. */
	private static final ConcurrentHashMap<String, VersionedConnectionPool<PoolableConnection>> DATASOURCE_COLLECTION = new ConcurrentHashMap<>();

	/**
	 * Instantiates a new Snowflake connection pool.
	 */
	private SnowflakeConnectionPool() {
		super();
	}

	/**
	 * Gets the pooled data source.
	 *
	 * @param connectionPoolSettings
	 *            the parameters needed to configure key for pooled data source
	 * @param properties
	 *            the properties needed to create pooled data source
	 * @return the pooled data source
	 */
	public static DataSource getPooledDataSource(ConnectionPoolSettings connectionPoolSettings, Properties properties) {
		String key = DigestUtils.sha256Hex(connectionPoolSettings.generateKey());
		VersionedConnectionPool<PoolableConnection> connectionPool;
		if (DATASOURCE_COLLECTION.get(key) == null) {
			connectionPool = createConnectionPool(connectionPoolSettings, properties);
			DATASOURCE_COLLECTION.putIfAbsent(key, connectionPool);
		}else {
			connectionPool = DATASOURCE_COLLECTION.get(key);
			if(connectionPool.isClosed()) {
				VersionedConnectionPool<PoolableConnection> newConnectionPool = createConnectionPool(connectionPoolSettings, properties);
				DATASOURCE_COLLECTION.replace(key, connectionPool, newConnectionPool);
				connectionPool = newConnectionPool;
			}
			connectionPool.updateLastAccessTime();
			if(connectionPoolSettings.equals(connectionPool)) {
				connectionPool.updatePoolSettings(connectionPoolSettings);
			}
		}
		return new PoolingDataSource<PoolableConnection>(DATASOURCE_COLLECTION.get(key));
	}

	/**
	 * Creates the connection pool.
	 *
	 * @param connectionPoolSettings
	 *            the parameters needed to configure key for pooled data source
	 * @param properties
	 *            the properties needed to create pooled data source
	 * @return connectionPool
	 * 			the connection pool object
	 */
	private static VersionedConnectionPool<PoolableConnection> createConnectionPool(ConnectionPoolSettings connectionPoolSettings, Properties properties) {
		
		ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectionPoolSettings.get_url(), properties);
		PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
		if(connectionPoolSettings.get_validationQuery() != null) {
			poolableConnectionFactory.setValidationQuery(connectionPoolSettings.get_validationQuery());
		}
		
		GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
		config.setMaxTotal(VersionedConnectionPool.scrubPoolLimit(connectionPoolSettings.get_maximumConnections()));
		int maxIdle = Math.min(VersionedConnectionPool.scrubPoolLimit(connectionPoolSettings.get_maximumConnections()), 50);
		config.setMaxIdle(maxIdle <= 0 ? 50 : maxIdle);
		config.setMinIdle(connectionPoolSettings.get_minimumConnections());
		if(connectionPoolSettings.get_whenExhaustedAction() == 1) {
			config.setMaxWaitMillis(0); // fail immediately if the pool is exhausted
		}else {
			config.setMaxWaitMillis(connectionPoolSettings.get_maximumWaitTime() * 1000L);
		}
		config.setTestOnBorrow(connectionPoolSettings.is_testOnBorrow());
		config.setTestOnReturn(connectionPoolSettings.is_testOnReturn());
		config.setTestWhileIdle(connectionPoolSettings.is_testWhileIdle());
		config.setNumTestsPerEvictionRun(-5);
		config.setMinEvictableIdleTimeMillis(connectionPoolSettings.get_maximumIdleTime() * 1000L);
		config.setTimeBetweenEvictionRunsMillis(300000L);
		config.setJmxEnabled(false);

		VersionedConnectionPool<PoolableConnection> connectionPool = new VersionedConnectionPool<>(poolableConnectionFactory, config);
		connectionPool.updateLastAccessTime();
		poolableConnectionFactory.setPool(connectionPool);

		logger.log(Level.INFO, "Connection pool created : total Connections : " + config.getMaxTotal() + ", Max Idle : "
				+ config.getMaxIdle() + ", Min Idle : " + config.getMinIdle());

		return connectionPool;
	}
	
	/**
	 * Evictor thread will run in every 30 minutes to check for the expired pool and close it.
	 */
	
    static {
        Runnable evictor = () -> {
			for (Entry<String, VersionedConnectionPool<PoolableConnection>> entry : DATASOURCE_COLLECTION.entrySet()) {
				String key = entry.getKey();
				VersionedConnectionPool<PoolableConnection> pool;
				VersionedConnectionPool<PoolableConnection> connectionPool = pool = entry.getValue();
				synchronized (connectionPool) {
					try {
						if (pool.isExpired() && !pool.isClosed()) {
							pool.close();
						}
						if (pool.isClosed()) {
							if (logger.isLoggable(Level.FINE)) {
								logger.log(Level.FINE, "Removing pool: {0} (last access time: {1})", new Object[]{pool, pool.getLastAccessTime()});
							}
							DATASOURCE_COLLECTION.remove(key, pool);
						}
					}
					catch (Exception e) {
						logger.log(Level.SEVERE, ("Unable to close connection pool: " + pool.toString()), e);
					}
				}
			}
		};
        SHUTDOWN_SERVICE.scheduleAtFixedRate(evictor, 30L, 30L, DEFAULT_EVICTION_INTERVAL_UNITS);
    }

}
