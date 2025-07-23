//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableDeleteFileAtPathAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableDeleteFileAtPathAction extends SingleRetryAction {

	/** The file full path. */
	private String fileFullPath;

	/**
	 * Instantiates a new retryable delete file at path action.
	 *
	 * @param connection the connection
	 * @param fileFullPath the file full path
	 * @param input the input
	 */
	public RetryableDeleteFileAtPathAction(SFTPConnection connection, String fileFullPath, TrackedData input) {
		super(connection, fileFullPath, input);
		this.fileFullPath = fileFullPath;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.getConnection().deleteFile(fileFullPath);
	}

}
