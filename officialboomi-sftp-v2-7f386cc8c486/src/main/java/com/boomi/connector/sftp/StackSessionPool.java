//Copyright (c) 2021 Boomi, Inc.
package com.boomi.connector.sftp;

import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import com.jcraft.jsch.Session;

/**
 * The Class StackSessionPool.
 * 
 * @author sweta.b.das
 */
public class StackSessionPool {

	/** The pool. */
	private KeyedObjectPool<ConnectionProperties, Session> pool;

	/**
	 * The Class SingletonHolder.
	 */
	private static class SingletonHolder {

		/** The Constant INSTANCE. */
		public static final StackSessionPool INSTANCE = new StackSessionPool();
	}

	/**
	 * Gets the single instance of StackSessionPool.
	 *
	 * @return single instance of StackSessionPool
	 */
	public static StackSessionPool getInstance() {
			return SingletonHolder.INSTANCE;
	}

	/**
	 * Instantiates a new stack session pool.
	 */
	private StackSessionPool() {
		startPool();
	}

	/**
	 * Gets the pool.
	 *
	 * @return the org.apache.commons.pool.KeyedObjectPool class
	 */
	public KeyedObjectPool<ConnectionProperties, Session> getPool() {
		return pool;
	}
	
	
	/**
	 * Start pool.
	 *
	 */
	private void startPool() {
		pool = new GenericKeyedObjectPool<ConnectionProperties, Session>(new SessionFactory(), 10);
	}
	
}