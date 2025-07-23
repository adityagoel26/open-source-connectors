//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableVerifyFileExistsAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableVerifyFileExistsAction extends SingleRetryAction {
	
	/** The full path. */
	private String fullPath;
	
	/** The file exists. */
	private Boolean fileExists;

	/**
	 * Gets the file exists.
	 *
	 * @return the file exists
	 */
	public Boolean getFileExists() {
		return fileExists;
	}

	/**
	 * Sets the file exists.
	 *
	 * @param fileExists the new file exists
	 */
	public void setFileExists(Boolean fileExists) {
		this.fileExists = fileExists;
	}

	/**
	 * Instantiates a new retryable verify file exists action.
	 *
	 * @param connection the connection
	 * @param fullPath the full path
	 * @param input the input
	 */
	public RetryableVerifyFileExistsAction(SFTPConnection connection, String fullPath,TrackedData input) {
		super(connection, null, input);
		this.fullPath = fullPath;
	}

	/**
	 * Instantiates a new retryable verify file exists action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 */
	public RetryableVerifyFileExistsAction(SFTPConnection connection, String remoteDir, String fileName) {
		super(connection, null, null);
		this.fullPath = connection.getPathsHandler().joinPaths(remoteDir, fileName);
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		fileExists = this.getConnection().fileExists(fullPath);
	}

}
