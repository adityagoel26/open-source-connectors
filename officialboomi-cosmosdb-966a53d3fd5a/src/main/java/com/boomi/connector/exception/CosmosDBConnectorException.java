//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.exception;

/**
 * @author Abhijit Mishra
 *
 *         ${tags}
 */
public class CosmosDBConnectorException extends Exception {

	private static final long serialVersionUID = 1L;

	private final String objectId;
	private final Integer errorCode;

	/**
	 * CosmosDBConnectorException Constructor
	 * @param message The Message.
	 * @param objectId The Object Id.
	 * @param errorCode
	 */
	public CosmosDBConnectorException(String message, String objectId, Integer errorCode) {
		super(message);
		this.objectId = objectId;
		this.errorCode = errorCode;
	}

	/**
	 * CosmosDBConnectorException Constructor
	 * @param message The Message.
	 */
	public CosmosDBConnectorException(String message) {
		super(message);
		this.objectId = null;
		this.errorCode = null;
	}

	/**
	 * CosmosDBConnectorException Constructor
	 * @param message The Message.
	 * @param objectId The Object Id.
	 * @param ex
	 */
	public CosmosDBConnectorException(String message, String objectId,Throwable ex) {
		super(message,ex);
		this.objectId = objectId;
		this.errorCode = null;
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
