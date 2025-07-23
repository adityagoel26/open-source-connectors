//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.retry.RetryStrategyFactory;

/**
 * The Class SingleRetryAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
abstract class SingleRetryAction extends RetryableAction {
	
	/** The Constant SINGLE_RETRY_FACTORY. */
	protected static final RetryStrategyFactory SINGLE_RETRY_FACTORY = RetryStrategyFactory.createFactory(1);

	/**
	 * Instantiates a new single retry action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param input the input
	 */
	SingleRetryAction(SFTPConnection connection, String remoteDir, TrackedData input) {
		super(connection, remoteDir, input, SINGLE_RETRY_FACTORY);
	}
}
