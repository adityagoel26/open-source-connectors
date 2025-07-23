// Copyright (c) 2024 Boomi, LP.
package com.boomi.connector.databaseconnector.pool;

import com.boomi.connector.databaseconnector.constants.DatabaseConnectorConstants;
import com.boomi.util.LogUtil;

import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.sql.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 * The Class ConnectionPoolSettings.
 */


public class VersionedConnectionPool<T>
extends GenericObjectPool<T>{
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(VersionedConnectionPool.class.getPackage().getName());
	
	/** The Last Access Time of the connection pool. */
	private volatile long _lastAccessTime = System.currentTimeMillis();
	
	/** The validation Query. */
	private String _validationQuery;

	public String get_validationQuery() {
		return _validationQuery;
	}

	public void set_validationQuery(String _validationQuery) {
		this._validationQuery = _validationQuery;
	}

	/**
	 * Instantiates a new Versioned connection pool.
	 * @param factory
	 * 			Pooled Object factory values
	 * @param config
	 * 			Generic Object pool Configuraton parameters
	 */
	public VersionedConnectionPool(PooledObjectFactory<T> factory, GenericObjectPoolConfig<T> config) {
		super(factory, config);
		if(((PoolableConnectionFactory) factory).getValidationQuery() != null)
			this._validationQuery = ((PoolableConnectionFactory) factory).getValidationQuery();
	}
	
	 /**
		 * Update the connection pool.
		 *
		 * @param connectionPoolSettings
		 *            the parameters needed to update the connection pool
		 */
	    protected synchronized void updatePoolSettings(ConnectionPoolSettings connectionPoolSettings) {

	        this.setMaxTotal(scrubPoolLimit(connectionPoolSettings.get_maximumConnections()));
	        int maxIdle = Math.min(scrubPoolLimit(connectionPoolSettings.get_maximumConnections()), 50);
	        this.setMaxIdle(maxIdle <= 0 ? 50 : maxIdle);
	        this.setMinIdle(connectionPoolSettings.get_minimumConnections());
			if(connectionPoolSettings.get_whenExhaustedAction() == 1) {
				this.setMaxWaitMillis(0); // fail immediately if the pool is exhausted
			}else
				this.setMaxWaitMillis(connectionPoolSettings.get_maximumWaitTime() * 1000L);
			this.setTestOnBorrow(connectionPoolSettings.is_testOnBorrow());
			this.setTestOnReturn(connectionPoolSettings.is_testOnReturn());
			this.setTestWhileIdle(connectionPoolSettings.is_testWhileIdle());
			this.setNumTestsPerEvictionRun(-5);
			this.setMinEvictableIdleTimeMillis(connectionPoolSettings.get_maximumIdleTime() * 1000L);
			this.setTimeBetweenEvictionRunsMillis(300000L);
			((PoolableConnectionFactory)this.getFactory()).setValidationQuery(connectionPoolSettings.get_validationQuery());
	        this.clear();
	        if (LOGGER.isLoggable(Level.FINE)) {
	            LogUtil.fine((Logger)LOGGER, (String)"Reconfigured pool");
	        }
	    }
	    /**
		 * Sets the default pool value if it is 0.
		 * @return 
		 * 		the pool parameters value
		 */
	    public static int scrubPoolLimit(int i) {
	        return i == 0 ? -1 : i;
	    }	
	    /**
		 * Sets the LastAccessTime.
		 */
	    public void updateLastAccessTime() {
	        this._lastAccessTime = System.currentTimeMillis();
	    }
	    /**
		 * Gets the LastAccessTime.
		 *
		 * @return the LastAccessTime.
		 */
		public Date getLastAccessTime() {
	        return new Date(this._lastAccessTime);
	    }

		/**
		 * Check if the pool has been expired. Pool will be expired if it has not been access for 6 hours.
		 * @return boolean
		 */
	    public synchronized boolean isExpired() {
	        return System.currentTimeMillis() - this._lastAccessTime > DatabaseConnectorConstants.LAST_ACCESS_TIME && this.getNumActive() <= 0;
	    }
}
