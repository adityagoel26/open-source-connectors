//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.exception;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBRetryException extends Exception {

	private static final long serialVersionUID = 1L;
	
	private final String objectId;
	private final Integer errorCode;

	/**
	 * CosmosDBRetryException Constructor
	 * @param message
	 * @param ex
	 */
	public CosmosDBRetryException(String message, Exception ex) {
		super(message, ex);
		this.objectId = null;
		this.errorCode = null;
	}

	/**
	 * CosmosDBRetryException Constructor
	 * @param message
	 */
	public CosmosDBRetryException(String message) {
		super(message);
		this.objectId = null;
		this.errorCode = null;
	}

	/**
	 * CosmosDBRetryException Constructor
	 * @param message
	 * @param objectId
	 * @param errorCode
	 */
	public CosmosDBRetryException(String message, String objectId, Integer errorCode) {
		super(message);
		this.objectId = objectId;
		this.errorCode = errorCode;
	}
	
	/**
	 * Returns the Object Id of the exception.
	 * @return objectId
	 */
	public String getObjectId() {
		return objectId;
	}

	/**
	 * Return the Error code of Exception
	 * @return errorCode
	 */
	public Integer getErrorCode() {
		return errorCode;
	}
}
