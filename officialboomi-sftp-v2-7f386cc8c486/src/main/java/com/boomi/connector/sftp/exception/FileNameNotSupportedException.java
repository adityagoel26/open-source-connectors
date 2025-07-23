//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.exception;

/**
 * The Class FileNameNotSupportedException.
 *
 */
public class FileNameNotSupportedException extends RuntimeException {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2183465319487851521L;

	/**
	 * Instantiates a new FileNameNotSupportedException
	 *
	 * @param statusMessage the status message
	 */
	public FileNameNotSupportedException(String statusMessage) {
		super(statusMessage);

	}

	/**
	 * Instantiates a new SFTP sdk exception.
	 *
	 * @param string the string
	 * @param e the e
	 */
	public FileNameNotSupportedException(String string, FileNameNotSupportedException e) {
		super(string, e);
	}

	/**
	 * Instantiates a new FileNameNotSupportedException
	 *
	 * @param errorMessage the error message
	 * @param err the err
	 */
	public FileNameNotSupportedException(String errorMessage, Throwable err) {
		super(errorMessage, err);
	}

	@Override
	public int hashCode() {
		return getMessage().hashCode();
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof FileNameNotSupportedException) {
			return getMessage().equalsIgnoreCase(((FileNameNotSupportedException) object).getMessage());
		}
		return getMessage().equals(object);
	}

}
