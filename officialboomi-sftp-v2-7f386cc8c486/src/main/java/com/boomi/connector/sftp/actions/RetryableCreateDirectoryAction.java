//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableCreateDirectoryAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableCreateDirectoryAction extends SingleRetryAction {
	
	/** The full path. */
	private String fullPath;
	

	/**
	 * Instantiates a new retryable create directory action.
	 *
	 * @param connection the connection
	 * @param fullPath the full path
	 * @param input the input
	 */
	public RetryableCreateDirectoryAction(SFTPConnection connection, String fullPath,TrackedData input) {
		super(connection, fullPath, input);
		this.fullPath = fullPath;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.getConnection().createNestedDirectories(fullPath);
	}

}
