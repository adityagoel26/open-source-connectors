//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.retry;

import com.boomi.util.retry.PhasedRetry;
import com.boomi.util.retry.RetryStrategy;

/**
 * A factory for creating LimitedRetryStrategy objects.
 *
 * @author Omesh Deoli
 * 
 * 
 */
final class LimitedRetryStrategyFactory extends RetryStrategyFactory {
	
	/** The max retries. */
	private final int maxRetries;

	/**
	 * Instantiates a new limited retry strategy factory.
	 *
	 * @param maxRetries the max retries
	 */
	LimitedRetryStrategyFactory(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	/**
	 * Creates a new LimitedRetryStrategy object.
	 *
	 * @return the retry strategy
	 */
	@Override
	public RetryStrategy createRetryStrategy() {
		return new PhasedRetry(this.maxRetries);
	}
}
