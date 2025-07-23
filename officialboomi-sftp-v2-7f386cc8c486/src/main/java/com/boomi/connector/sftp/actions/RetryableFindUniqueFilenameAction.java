//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;

import com.boomi.connector.sftp.SFTPConnection;

/**
 * The Class RetryableFindUniqueFilenameAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableFindUniqueFilenameAction extends SingleRetryAction {
	
	/** The unique file name. */
	private String uniqueFileName;
	
	/**
	 * Gets the unique file name.
	 *
	 * @return the unique file name
	 */
	public String getUniqueFileName() {
		return uniqueFileName;
	}

	/**
	 * Sets the unique file name.
	 *
	 * @param uniqueFileName the new unique file name
	 */
	public void setUniqueFileName(String uniqueFileName) {
		this.uniqueFileName = uniqueFileName;
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


	/** The entered file name. */
	private String enteredFileName;
	
	/** The remote dir. */
	private String remoteDir;

	
	/**
	 * Instantiates a new retryable find unique filename action.
	 *
	 * @param connection the connection
	 * @param enteredFileName the entered file name
	 * @param remoteDir the remote dir
	 * @param input the input
	 */
	public RetryableFindUniqueFilenameAction(SFTPConnection connection, String enteredFileName, String remoteDir,TrackedData input) {
		super(connection, remoteDir, input);
	    this.remoteDir=remoteDir;
	    this.enteredFileName=enteredFileName;
		
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		uniqueFileName = this.getConnection().findUniqueFileName(enteredFileName, remoteDir);
	}

}
