// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp.retry;


import org.junit.Test;


public class RetryStrategyFactoryTest {

	@Test
	public void testCreateRetryStrategy() {
		RetryStrategyFactory bc=RetryStrategyFactory.createFactory(0);
		RetryStrategyFactory ab=RetryStrategyFactory.createFactory(-1);
		RetryStrategyFactory cd=RetryStrategyFactory.createFactory(2);
		ab.createRetryStrategy().shouldRetry(1, "abc");
		
		bc.createRetryStrategy();
		cd.createRetryStrategy();
	
	}

	@Test(expected=UnsupportedOperationException.class)
	public void testCreateFactory() {
		RetryStrategyFactory ab=RetryStrategyFactory.createFactory(-1);
		ab.createRetryStrategy().backoff(1);
	}

}
