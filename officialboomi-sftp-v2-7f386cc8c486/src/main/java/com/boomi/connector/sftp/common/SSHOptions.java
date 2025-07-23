//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;

import com.boomi.connector.sftp.constants.SFTPConstants;

/**
 * The Class SSHOptions.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class SSHOptions {

	/** The known host entry. */
	private String knownHostEntry;

	/** The sshkeyauth. */
	private Boolean sshkeyauth;

	/** The sshkeypath. */
	private String sshkeypath;

	/** The sshkeypassword. */
	private String sshkeypassword;

	/** The dh key size max 1024. */
	private Boolean dhKeySizeMax1024;

	/**
	 * Gets the known host entry.
	 *
	 * @return the known host entry
	 */
	public String getKnownHostEntry() {
		return knownHostEntry;
	}
	
	/**
	 * Instantiates a new SSH options.
	 *
	 * @param properties the properties
	 */
	public SSHOptions(PropertiesUtil properties)
	{
		this.knownHostEntry=properties.getKnownHostEntry();
		this.sshkeyauth=properties.getAuthType().equals(SFTPConstants.USING_PUBLIC_KEY);
		this.sshkeypath=properties.getprvtkeyPath();
		this.sshkeypassword=properties.getpassphrase();
		this.dhKeySizeMax1024=properties.isMaxExchangeEnabled();
	}

	/**
	 * Sets the known host entry.
	 *
	 * @param knownHostEntry the new known host entry
	 */
	public void setKnownHostEntry(String knownHostEntry) {
		this.knownHostEntry = knownHostEntry;
	}

	/**
	 * Checks if is sshkeyauth.
	 *
	 * @return the boolean
	 */
	public Boolean isSshkeyauth() {
		return sshkeyauth;
	}

	/**
	 * Sets the sshkeyauth.
	 *
	 * @param sshkeyauth the new sshkeyauth
	 */
	public void setSshkeyauth(Boolean sshkeyauth) {
		this.sshkeyauth = sshkeyauth;
	}

	/**
	 * Gets the sshkeypath.
	 *
	 * @return the sshkeypath
	 */
	public String getSshkeypath() {
		return sshkeypath;
	}

	/**
	 * Sets the sshkeypath.
	 *
	 * @param sshkeypath the new sshkeypath
	 */
	public void setSshkeypath(String sshkeypath) {
		this.sshkeypath = sshkeypath;
	}

	/**
	 * Gets the sshkeypassword.
	 *
	 * @return the sshkeypassword
	 */
	public String getSshkeypassword() {
		return sshkeypassword;
	}

	/**
	 * Sets the sshkeypassword.
	 *
	 * @param sshkeypassword the new sshkeypassword
	 */
	public void setSshkeypassword(String sshkeypassword) {
		this.sshkeypassword = sshkeypassword;
	}

	/**
	 * Checks if is dh key size max 1024.
	 *
	 * @return the boolean
	 */
	public Boolean isDhKeySizeMax1024() {
		return dhKeySizeMax1024;
	}

	/**
	 * Dh key size max 1024.
	 *
	 * @param dhKeySizeMax1024 the dh key size max 1024
	 */
	public void DhKeySizeMax1024(Boolean dhKeySizeMax1024) {
		this.dhKeySizeMax1024 = dhKeySizeMax1024;
	}
}
