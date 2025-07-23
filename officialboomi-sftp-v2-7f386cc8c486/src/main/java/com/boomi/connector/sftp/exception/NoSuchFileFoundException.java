//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.exception;

/**
  * The Class NoSuchFileFoundException.
  *
  * @author Omesh Deoli
  * 
  * 
  */
public class NoSuchFileFoundException extends RuntimeException {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -6908750692873284245L;

	/**
	 * Instantiates a new no such file found exception.
	 *
	 * @param statusMessage the status message
	 */
	public NoSuchFileFoundException(String statusMessage) {
		super(statusMessage);
		
	}

	/**
	 * Instantiates a new no such file found exception.
	 *
	 * @param errMessage the err message
	 * @param e the e
	 */
	public NoSuchFileFoundException(String errMessage, Throwable e) {
		super(errMessage, e);
	}

}
