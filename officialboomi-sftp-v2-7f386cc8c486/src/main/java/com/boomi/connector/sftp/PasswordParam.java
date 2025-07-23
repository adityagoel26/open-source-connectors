//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

/**
 * The Class PasswordParam.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class PasswordParam {

	/** The user. */
	private String user;
	
	/** The password. */
	private String password;

	/**
	 * Instantiates a new password param.
	 *
	 * @param user the user
	 * @param password the password
	 */
	public PasswordParam(String user, String password) {
		super();
		this.user = user;
		this.password = password;
	}

	/**
	 * Gets the user.
	 *
	 * @return the user
	 */
	public String getUser() {
		return user;
	}

	/**
	 * Sets the user.
	 *
	 * @param user the new user
	 */
	public void setUser(String user) {
		this.user = user;
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Sets the password.
	 *
	 * @param password the new password
	 */
	public void setPassword(String password) {
		this.password = password;
	}

}
