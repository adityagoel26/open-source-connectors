//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.exception;

import com.jcraft.jsch.SftpException;

/**
 * The Class SFTPSdkException.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SFTPSdkException extends RuntimeException {

	/** The Constant serialVersionUID. */
	private static final long serialVersionUID = -2183465319487851521L;

	/**
	 * Instantiates a new SFTP sdk exception.
	 *
	 * @param statusMessage the status message
	 */
	public SFTPSdkException(String statusMessage) {
		super(statusMessage);

	}

	/**
	 * Instantiates a new SFTP sdk exception.
	 *
	 * @param string the string
	 * @param e the e
	 */
	public SFTPSdkException(String string, SftpException e) {
		super(string, e);
	}

	/**
	 * Instantiates a new SFTP sdk exception.
	 *
	 * @param errorMessage the error message
	 * @param err the err
	 */
	public SFTPSdkException(String errorMessage, Throwable err) {
		super(errorMessage, err);
	}

	@Override
	public int hashCode() {
		return getMessage().hashCode();
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof SFTPSdkException) {
			return getMessage().equalsIgnoreCase(((SFTPSdkException) object).getMessage());
		}
		return getMessage().equals(object);
	}
}
