// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.oracledatabase.pool;

import java.sql.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import static com.boomi.connector.oracledatabase.util.OracleDatabaseConstants.*;

import com.boomi.util.LogUtil;
/**
 * The Class ConnectionPoolSettings.
 */


public class VersionedConnectionPool<T>
extends GenericObjectPool<T>{
	
	/** The Constant LOGGER. */
	private static final Logger LOGGER = Logger.getLogger(VersionedConnectionPool.class.getPackage().getName());
	
	/** The Last Access Time of the connection pool. */
	private volatile long lastAccessTime = System.currentTimeMillis();
	
	/** The validation Query. */
	private String validationQuery;

	/** The gets the validation Query. */
	public String getValidationQuery() {
		return validationQuery;
	}

	/** The sets the validation Query. */
	public void setValidationQuery(String validationQuery) {
		this.validationQuery = validationQuery;
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
			this.validationQuery = ((PoolableConnectionFactory) factory).getValidationQuery();
	}
	
	 /**
		 * Update the connection pool.
		 *
		 * @param connectionPoolSettings
		 *            the parameters needed to update the connection pool
		 */
	    protected synchronized void updatePoolSettings(ConnectionPoolSettings connectionPoolSettings) {

	        this.setMaxTotal(scrubPoolLimit(connectionPoolSettings.getMaximumConnections()));
	        int maxIdle = Math.min(scrubPoolLimit(connectionPoolSettings.getMaximumConnections()), 50);
	        this.setMaxIdle(maxIdle <= 0 ? 50 : maxIdle);
	        this.setMinIdle(connectionPoolSettings.getMinimumConnections());
			if(connectionPoolSettings.getWhenExhaustedAction() == 1) {
				this.setMaxWaitMillis(0); // fail immediately if the pool is exhausted
			}else
				this.setMaxWaitMillis(connectionPoolSettings.getMaximumWaitTime() * 1000L);
			this.setTestOnBorrow(connectionPoolSettings.isTestOnBorrow());
			this.setTestOnReturn(connectionPoolSettings.isTestOnReturn());
			this.setTestWhileIdle(connectionPoolSettings.isTestWhileIdle());
			this.setNumTestsPerEvictionRun(-5);
			this.setMinEvictableIdleTimeMillis(connectionPoolSettings.getMaximumIdleTime() * 1000L);
			this.setTimeBetweenEvictionRunsMillis(300000L);
			((PoolableConnectionFactory)this.getFactory()).setValidationQuery(connectionPoolSettings.getValidationQuery());
	        this.clear();
	        if (LOGGER.isLoggable(Level.FINE)) {
	            LogUtil.fine(LOGGER,"Reconfigured pool");
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
	        this.lastAccessTime = System.currentTimeMillis();
	    }
	    /**
		 * Gets the LastAccessTime.
		 *
		 * @return the LastAccessTime.
		 */
		public Date getLastAccessTime() {
	        return new Date(this.lastAccessTime);
	    }

		/**
		 * Check if the pool has been expired. Pool will be expired if it has not been access for 6 hours.
		 * @return boolean
		 */
	    public synchronized boolean isExpired() {
	        return System.currentTimeMillis() - this.lastAccessTime > LAST_ACCESS_TIME && this.getNumActive() <= 0;
	    }
}
