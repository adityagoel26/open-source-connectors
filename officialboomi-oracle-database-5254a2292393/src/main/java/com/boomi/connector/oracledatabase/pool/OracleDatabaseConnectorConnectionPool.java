// Copyright (c) 2022 Boomi, LP.
package com.boomi.connector.oracledatabase.pool;

import java.util.Properties;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;
import com.boomi.util.ExecutorUtil;
/**
 * The Class ConnectionPoolSettings.
 */

public class OracleDatabaseConnectorConnectionPool {
	
	
	/** The Constant DEFAULT_EVICTION_INTERVAL_UNITS. */
	private static final TimeUnit DEFAULT_EVICTION_INTERVAL_UNITS = TimeUnit.MINUTES;
	
	/** The ScheduledExecutorService SHUTDOWN_SERVICE . */
	private static final ScheduledExecutorService SHUTDOWN_SERVICE = ExecutorUtil.newScheduler("DatabaseConnector Pool Shutdown Service");

	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(OracleDatabaseConnectorConnectionPool.class.getName());

	/** The ConcurrentHashMap DATASOURCE_COLLECTION. */
	private static final ConcurrentHashMap<String, VersionedConnectionPool<PoolableConnection>> DATASOURCE_COLLECTION = new ConcurrentHashMap<>();

	
		/**
		 * Instantiates a new DatabaseConnector connection pool.
		 */
		private OracleDatabaseConnectorConnectionPool() {
			
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
				if(connectionPoolSettings.isChanged(connectionPool)) {
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
			
			ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(connectionPoolSettings.getUrl(), properties);
			PoolableConnectionFactory poolableConnectionFactory = new PoolableConnectionFactory(connectionFactory, null);
			if(connectionPoolSettings.getValidationQuery() != null)
				poolableConnectionFactory.setValidationQuery(connectionPoolSettings.getValidationQuery());
			
			GenericObjectPoolConfig<PoolableConnection> config = new GenericObjectPoolConfig<>();
			config.setMaxTotal(VersionedConnectionPool.scrubPoolLimit(connectionPoolSettings.getMaximumConnections()));
			int maxIdle = Math.min(VersionedConnectionPool.scrubPoolLimit(connectionPoolSettings.getMaximumConnections()), 50);
			config.setMaxIdle(maxIdle <= 0 ? MAX_IDLE : maxIdle);
			config.setMinIdle(connectionPoolSettings.getMinimumConnections());
			if(connectionPoolSettings.getWhenExhaustedAction() == 1) {
				config.setMaxWaitMillis(0); // fail immediately if the pool is exhausted
			}else
				config.setMaxWaitMillis(connectionPoolSettings.getMaximumWaitTime() * 1000L);
			config.setTestOnBorrow(connectionPoolSettings.isTestOnBorrow());
			config.setTestOnReturn(connectionPoolSettings.isTestOnReturn());
			config.setTestWhileIdle(connectionPoolSettings.isTestWhileIdle());
			config.setNumTestsPerEvictionRun(-5);
			config.setMinEvictableIdleTimeMillis(connectionPoolSettings.getMaximumIdleTime() * 1000L);
			config.setTimeBetweenEvictionRunsMillis(300000L);
			config.setJmxEnabled(false);

			VersionedConnectionPool<PoolableConnection> connectionPool = new VersionedConnectionPool<>(poolableConnectionFactory, config);
			connectionPool.updateLastAccessTime();
			poolableConnectionFactory.setPool(connectionPool);
			String logMsg = new StringBuffer().append("Connection pool created : total Connections : ")
					.append(config.getMaxTotal()).append(", Max Idle : ")
					.append(config.getMaxIdle()).append(", Min Idle : ")
					.append(config.getMinIdle()).toString();
			logger.log(Level.INFO, logMsg);

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
							logger.log(Level.SEVERE, ("Unable to close connection pool: " + pool.toString()), (Throwable)e);
						}
					}
				}
			};
	        SHUTDOWN_SERVICE.scheduleAtFixedRate(evictor, 30L, 30L, DEFAULT_EVICTION_INTERVAL_UNITS);
	    }
		
	}



