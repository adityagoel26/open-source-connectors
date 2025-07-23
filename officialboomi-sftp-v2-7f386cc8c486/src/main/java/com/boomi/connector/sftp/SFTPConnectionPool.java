//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp;

import com.boomi.util.ExecutorUtil;
import com.jcraft.jsch.Session;

import org.apache.commons.pool.KeyedObjectPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The Class SFTPConnectionPool.
 *
 * @author sweta.b.das
 * 
 * 
 */
public class SFTPConnectionPool {

	/** The log. */
	private static Logger log = Logger.getLogger(SFTPConnectionPool.class.getName());

	/** The connection map. */
	private static Map<String, ConnectionProperties> connectionMap = new HashMap<>();

	/** The Constant DEFAULT_EVICTION_INTERVAL_UNITS. */
	private static final TimeUnit DEFAULT_EVICTION_INTERVAL_UNITS = TimeUnit.MINUTES;

	/** The ScheduledExecutorService SHUTDOWN_SERVICE . */
	private static final ScheduledExecutorService SHUTDOWN_SERVICE = ExecutorUtil
			.newScheduler((String) "SFTP Pool Shutdown Service");

	/** The connection tracking map. */
	private static Map<ConnectionProperties, KeyedObjectPool<ConnectionProperties, Session>> connectionTrackingMap = new HashMap<>();

	private SFTPConnectionPool() {
		//Hide implicit constructor
	}

	/**
	 * Gets the connection propeties.
	 *
	 * @param conProp the con prop
	 * @param key the key
	 * @return the connection propeties
	 */
	public static ConnectionProperties getConnectionPropeties(ConnectionProperties conProp, String key) {
		if (connectionMap.get(key) == null) {
			conProp.setCurrentDate(System.currentTimeMillis());
			connectionMap.put(key, conProp);
			connectionTrackingMap.put(conProp, StackSessionPool.getInstance().getPool());
		} else {
			if (key != null) {
				conProp = connectionMap.get(key);
				conProp.setCurrentDate(System.currentTimeMillis());
			}
		}
		return conProp;
	}

	static {
		Runnable evictor = () -> {
			for (Entry<String, ConnectionProperties> entry : connectionMap.entrySet()) {
				String key = entry.getKey();
				KeyedObjectPool<ConnectionProperties, Session> pool;
				KeyedObjectPool<ConnectionProperties, Session> connectionPool = pool = connectionTrackingMap
						.get(entry.getValue());
				synchronized (connectionPool) {
					try {
						if (System.currentTimeMillis() - entry.getValue().getCurrentDate() > 10800000 && !(pool.getNumActive() > 0)) {
							pool.clear();
							connectionMap.remove(key);
							connectionTrackingMap.remove(entry.getValue());
						}
					} catch (Exception e) {
						log.log(Level.SEVERE, (String) ("Unable to close connection pool: " + pool.toString()),
								(Throwable) e);
					}
				}
			}
		};
		SHUTDOWN_SERVICE.scheduleAtFixedRate(evictor, 30L, 30L, DEFAULT_EVICTION_INTERVAL_UNITS);
	}

}
