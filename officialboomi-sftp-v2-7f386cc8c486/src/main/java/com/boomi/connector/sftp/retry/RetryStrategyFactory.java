//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.retry;

import com.boomi.util.retry.NeverRetry;
import com.boomi.util.retry.RetryStrategy;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A factory for creating RetryStrategy objects.
 *
 * @author Omesh Deoli
 * 
 *
 */
public abstract class RetryStrategyFactory {
	
	/** The Constant ALWAYS_RETRY_STRATEGY. */
	private static final RetryStrategy ALWAYS_RETRY_STRATEGY = new RetryStrategy() {

		public boolean shouldRetry(int retryNumber, Object status) {
			Logger logger = Logger.getLogger(RetryStrategyFactory.class.getName());
			logger.log(Level.FINE,"retryNumber {0}", retryNumber);
			logger.log(Level.FINE,"status {0}", status);
			return true;
		}

		public void backoff(int retryNumber) {
			throw new UnsupportedOperationException();
		}
	};

	/**
	 * Creates a new RetryStrategy object.
	 *
	 * @return the retry strategy
	 */
	public abstract RetryStrategy createRetryStrategy();

	/**
	 * Creates a new RetryStrategy object.
	 *
	 * @param maxRetries the max retries
	 * @return the retry strategy factory
	 */
	public static RetryStrategyFactory createFactory(int maxRetries) {
		if(maxRetries == -1) {
			return new AlwaysRetryStrategyFactory();
		}
		if(maxRetries == 0) {
			return new NeverRetryStrategyFactory();
		}
		else {
		return new LimitedRetryStrategyFactory(maxRetries);
		}
	}

	/**
	 * A factory for creating NeverRetryStrategy objects.
	 */
	static final class NeverRetryStrategyFactory extends RetryStrategyFactory {
		
		/**
		 * Instantiates a new never retry strategy factory.
		 */
		NeverRetryStrategyFactory() {
		}

		/**
		 * Creates a new NeverRetryStrategy object.
		 *
		 * @return the retry strategy
		 */
		@Override
		public RetryStrategy createRetryStrategy() {
			return NeverRetry.INSTANCE;
		}
	}

	/**
	 * A factory for creating AlwaysRetryStrategy objects.
	 */
	static final class AlwaysRetryStrategyFactory extends RetryStrategyFactory {
		
		/**
		 * Instantiates a new always retry strategy factory.
		 */
		AlwaysRetryStrategyFactory() {
		}

		/**
		 * Creates a new AlwaysRetryStrategy object.
		 *
		 * @return the retry strategy
		 */
		@Override
		public RetryStrategy createRetryStrategy() {
			return ALWAYS_RETRY_STRATEGY;
		}
	}

}
