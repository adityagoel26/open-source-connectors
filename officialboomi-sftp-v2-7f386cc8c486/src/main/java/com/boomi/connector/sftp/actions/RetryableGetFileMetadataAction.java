//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.SFTPConnection;
import com.jcraft.jsch.SftpATTRS;

/**
 * The Class RetryableGetFileMetadataAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableGetFileMetadataAction extends SingleRetryAction {
	
	/** The file name. */
	private String fileName;
	
	/** The file meta data. */
	private SftpATTRS fileMetaData;

	/**
	 * Sets the file name.
	 *
	 * @param fileName the new file name
	 */
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	/**
	 * Gets the file name.
	 *
	 * @return the file name
	 */
	public String getFileName() {
		return fileName;
	}

	/**
	 * Gets the file meta data.
	 *
	 * @return the file meta data
	 */
	public SftpATTRS getFileMetaData() {
		return fileMetaData;
	}

	/**
	 * Instantiates a new retryable get file metadata action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param input the input
	 * @param fileName the file name
	 */
	public RetryableGetFileMetadataAction(SFTPConnection connection, String remoteDir, TrackedData input,
			String fileName) {
		super(connection, remoteDir, input);
		this.fileName = fileName;
	}

	/**
	 * Instantiates a new retryable get file metadata action.
	 *
	 * @param connection the connection
	 * @param remoteDir the remote dir
	 * @param fileName the file name
	 */
	public RetryableGetFileMetadataAction(SFTPConnection connection, String remoteDir, 
			String fileName) {
		super(connection, remoteDir, null);
		this.fileName = fileName;
	}
	
	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		this.fileMetaData = this.getConnection().getSingleFileAttributes(this.getRemoteDir(), fileName);

	}
}
