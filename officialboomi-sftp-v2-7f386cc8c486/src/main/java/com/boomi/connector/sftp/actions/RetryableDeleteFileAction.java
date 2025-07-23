//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.results.Result;

/**
 * The Class RetryableDeleteFileAction.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class RetryableDeleteFileAction extends SingleRetryAction {
	
	/** The file id. */
	private ObjectIdData fileId;
	
	/** The result. */
	private Result result;


	/**
	 * Instantiates a new retryable delete file action.
	 *
	 * @param connection the connection
	 * @param input the input
	 */
	public RetryableDeleteFileAction(SFTPConnection connection, ObjectIdData input) {
		super(connection, null, input);
		this.fileId = input;
	}

	/**
	 * Do execute.
	 */
	@Override
	public void doExecute() {
		result = this.getConnection().deleteFile(fileId);
	}

	/**
	 * Gets the result.
	 *
	 * @return the result
	 */
	public Result getResult() {
		return result;
	}

	/**
	 * Sets the result.
	 *
	 * @param result the new result
	 */
	public void setResult(Result result) {
		this.result = result;
	}
}
