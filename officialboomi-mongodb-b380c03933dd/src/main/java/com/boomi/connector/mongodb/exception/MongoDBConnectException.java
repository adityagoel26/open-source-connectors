// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb.exception;

/**
 * The Class MongoDBConnectException.
 *
 */
public class MongoDBConnectException extends Exception {
	
	/**
	 * The serialVersionUID.
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Instantiates a new mongo DB connect exception.
	 *
	 * @param message the message
	 */
	public MongoDBConnectException(String message)	{
		super(message);	
	}

	/**
	 * Instantiates a new mongo DB connect exception.
	 *
	 * @param string the string
	 * @param ex the ex
	 */
	public MongoDBConnectException(String string, Exception ex) {
		super(string, ex);
	}

	/**
	 * Instantiates a new mongo DB connect exception.
	 *
	 * @param ex the ex
	 */
	public MongoDBConnectException(Exception ex) {
		super(ex);
	}

}