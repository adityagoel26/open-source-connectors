//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.sftp.constants.SFTPConstants;

/**
 * The Enum AuthType.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public enum AuthType {

	/** The password. */
	PASSWORD(SFTPConstants.USERNAME_AND_PASSWORD), /** The public key. */
 PUBLIC_KEY("Using public Key");

	/** The auth type desc. */
	private String authTypeDesc;

	/**
	 * Instantiates a new auth type.
	 *
	 * @param p the p
	 */
	AuthType(String p) {
		authTypeDesc = p;
	}

	/**
	 * Gets the auth type.
	 *
	 * @return the auth type
	 */
	public String getAuthType() {
		return authTypeDesc;
	}
}
