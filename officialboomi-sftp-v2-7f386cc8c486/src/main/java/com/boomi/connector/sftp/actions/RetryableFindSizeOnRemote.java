//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableFindSizeOnRemote.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableFindSizeOnRemote extends SingleRetryAction {

	/** The file size. */
	private Long fileSize;

	/** The entered file name. */
	private String enteredFileName;
	
	/** The remote dir. */
	private String remoteDir;

	/**
	 * Gets the file size.
	 *
	 * @return the file size
	 */
	public Long getFileSize() {
		return fileSize;
	}

	/**
	 * Sets the file size.
	 *
	 * @param fileSize the new file size
	 */
	public void setFileSize(Long fileSize) {
		this.fileSize = fileSize;
	}

	/**
	 * Gets the entered file name.
	 *
	 * @return the entered file name
	 */
	public String getEnteredFileName() {
		return enteredFileName;
	}

	/**
	 * Sets the entered file name.
	 *
	 * @param enteredFileName the new entered file name
	 */
	public void setEnteredFileName(String enteredFileName) {
		this.enteredFileName = enteredFileName;
	}

	/**
	 * Gets the remote dir.
	 *
	 * @return the remote dir
	 */
	@Override
	public String getRemoteDir() {
		return remoteDir;
	}

	
	/**
	 * Instantiates a new retryable find size on remote.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param enteredFileName the entered file name
	 * @param input the input
	 */
	public RetryableFindSizeOnRemote(SFTPConnection connection, String remoteDir,String enteredFileName, TrackedData input) {
		super(connection, remoteDir, input);
		this.remoteDir = remoteDir;
		this.enteredFileName = enteredFileName;

	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		fileSize = this.getConnection().getSizeOnRemote(remoteDir, enteredFileName);
	}

}
