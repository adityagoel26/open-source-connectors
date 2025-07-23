//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.results;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.exception.NoSuchFileFoundException;
import com.boomi.connector.sftp.exception.SFTPSdkException;

/**
 * The Class ErrorResult.
 *
 * @author Omesh Deoli
 * 
 *
 */
public class ErrorResult extends BaseResult {
	
	/** The throwable. */
	private final Throwable throwable;

	/**
	 * Instantiates a new error result.
	 *
	 * @param status the status
	 * @param e the e
	 */
	public ErrorResult(OperationStatus status, Exception e) {
		super(null, ErrorResult.inferCode(e), e.getMessage(), status != null ? status : ErrorResult.inferStatus(e));
		this.throwable = e;
	}

	/**
	 * Instantiates a new error result.
	 *
	 * @param e the e
	 */
	public ErrorResult(Exception e) {
		this(null, e);
	}

	/**
	 * Adds the to response.
	 *
	 * @param response the response
	 * @param input the input
	 */
	@Override
	public void addToResponse(OperationResponse response, TrackedData input) {
		response.addErrorResult(input, this.getStatus(), this.getStatusCode(), this.getStatusMessage(),
				this.throwable);
	}

	/**
	 * Infer code.
	 *
	 * @param e the e
	 * @return the string
	 */
	public static String inferCode(Exception e) {
		if (e instanceof NoSuchFileFoundException) {
			return "1";
		}
		return "-1";
	}

	/**
	 * Infer status.
	 *
	 * @param e the e
	 * @return the operation status
	 */
	public static OperationStatus inferStatus(Exception e) {
		if (e instanceof SFTPSdkException || e instanceof NoSuchFileFoundException) {
			return OperationStatus.APPLICATION_ERROR;
		}
		return OperationStatus.FAILURE;
	}
}
