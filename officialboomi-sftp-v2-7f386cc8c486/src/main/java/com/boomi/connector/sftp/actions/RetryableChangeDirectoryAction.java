//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableChangeDirectoryAction.
 *
 * @author Omesh Deoli
 * 
 */
public class RetryableChangeDirectoryAction extends SingleRetryAction {
	
	/** The full path. */
	private String fullPath;
	
	/**
	 * Instantiates a new retryable change directory action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 * @param input the input
	 */
	public RetryableChangeDirectoryAction(SFTPConnection connection, String remoteDir, String fileName,
			TrackedData input) {
		super(connection, remoteDir, input);

	}

	/**
	 * Instantiates a new retryable change directory action.
	 *
	 * @param connection the connection
	 * @param fullPath the full path
	 * @param input the input
	 */
	public RetryableChangeDirectoryAction(SFTPConnection connection, String fullPath,TrackedData input) {
		super(connection, fullPath, input);
		this.fullPath = fullPath;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.getConnection().changeCurrentDirectory(fullPath);
	}

}
