//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableRenameFileAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableRenameFileAction extends SingleRetryAction {
	
	/** The original path. */
	private final String originalPath;
	
	/** The new path. */
	private final String newPath;

	/**
	 * Instantiates a new retryable rename file action.
	 *
	 * @param connection the connection
	 * @param originalPath the original path
	 * @param newPath the new path
	 * @param input the input
	 */
	public RetryableRenameFileAction(SFTPConnection connection, String originalPath, String newPath,TrackedData input) {
		super(connection, null, input);
		this.originalPath = originalPath;
		this.newPath = newPath;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.getConnection().renameFile(this.originalPath, this.newPath);
	}
}
