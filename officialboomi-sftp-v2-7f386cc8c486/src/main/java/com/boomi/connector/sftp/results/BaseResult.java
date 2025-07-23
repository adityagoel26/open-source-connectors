//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.results;

import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.constants.SFTPConstants;

/**
 * The Class BaseResult.
 *
 * @author Omesh Deoli
 * 
 *
 */
public class BaseResult implements Result {

	/** The status code. */
	private final String statusCode;
	
	/** The status message. */
	private final String statusMessage;
	
	/** The status. */
	private final OperationStatus status;
	
	/** The payload. */
	private final Payload payload;

	/**
	 * Instantiates a new base result.
	 *
	 * @param payload the payload
	 * @param statusCode the status code
	 * @param message the message
	 * @param status the status
	 */
	public BaseResult(Payload payload, String statusCode, String message, OperationStatus status) {
		this.payload = payload;
		this.status = status;
		this.statusCode = statusCode;
		this.statusMessage = message;
	}

	/**
	 * Instantiates a new base result.
	 *
	 * @param payload the payload
	 */
	public BaseResult(Payload payload) {
		this(payload, SFTPConstants.SUCCESS_CODE, SFTPConstants.SUCCESS_MESSAGE, OperationStatus.SUCCESS);
	}

	/**
	 * Adds the to response.
	 *
	 * @param response the response
	 * @param input the input
	 */
	@Override
	public void addToResponse(OperationResponse response, TrackedData input) {
		response.addResult(input, this.status, this.statusCode, this.statusMessage, this.payload);
	}

	/**
	 * Gets the status code.
	 *
	 * @return the status code
	 */
	public String getStatusCode() {
		return this.statusCode;
	}

	/**
	 * Gets the status message.
	 *
	 * @return the status message
	 */
	public String getStatusMessage() {
		return this.statusMessage;
	}

	/**
	 * Gets the status.
	 *
	 * @return the status
	 */
	public OperationStatus getStatus() {
		return this.status;
	}

	/**
	 * Gets the payload.
	 *
	 * @return the payload
	 */
	public Payload getPayload() {
		return this.payload;
	}

}
