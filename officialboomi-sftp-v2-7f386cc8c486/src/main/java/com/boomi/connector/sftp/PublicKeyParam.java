//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp;

import java.nio.charset.StandardCharsets;

/**
 * The Class PublicKeyParam.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class PublicKeyParam {

	/** The passphrase. */
	private String passphrase;
	
	/** The prvkey path. */
	private String prvkeyPath;
	
	/** The user. */
	private String user;

	/** The prvkey content. */
	private byte[] prvkeyContent;
	
	/** The pubkey content. */
	private byte[] pubkeyContent;
	
	/** The passphrase content. */
	private byte[] passphraseContent;
	
	/** The key pair name. */
	private String keyPairName;
	
	/** The use key content. */
	private boolean useKeyContent;

	/**
	 * Gets the passphrase content.
	 *
	 * @return the passphrase content
	 */
	public byte[] getPassphraseContent() {
		return passphraseContent;
	}

	/**
	 * Sets the passphrase content.
	 *
	 * @param passphraseContent the new passphrase content
	 */
	public void setPassphraseContent(byte[] passphraseContent) {
		this.passphraseContent = passphraseContent;
	}

	/**
	 * Instantiates a new public key param.
	 *
	 * @param passphrase the passphrase
	 * @param prvkeyPath the prvkey path
	 * @param user the user
	 * @param useKeyContent the use key content
	 */
	public PublicKeyParam(String passphrase, String prvkeyPath, String user, boolean useKeyContent) {
		super();
		this.passphrase = passphrase;
		this.prvkeyPath = prvkeyPath;
		this.user = user;
		this.useKeyContent = useKeyContent;
	}

	/**
	 * Instantiates a new public key param.
	 *
	 * @param user the user
	 * @param passphrase the passphrase
	 * @param prvkeyContent the prvkey content
	 * @param pubkeyContent the pubkey content
	 * @param keyPairName the key pair name
	 * @param useKeyContent the use key content
	 */
	public PublicKeyParam(String user,String passphrase, String prvkeyContent, String pubkeyContent, String keyPairName,
			boolean useKeyContent) {
		super();
		this.passphraseContent = passphrase.getBytes(StandardCharsets.UTF_8);
		this.prvkeyContent = formatPrivateKey(prvkeyContent).getBytes(StandardCharsets.UTF_8);
		this.pubkeyContent = formatPublicKey(pubkeyContent).getBytes(StandardCharsets.UTF_8);
		this.keyPairName = keyPairName;
		this.useKeyContent = useKeyContent;
		this.user=user;	
	}

	/**
	 * Checks if is use key content enabled.
	 *
	 * @return true, if is use key content enabled
	 */
	public boolean isUseKeyContentEnabled() {
		return useKeyContent;
	}

	/**
	 * Sets the use key content.
	 *
	 * @param useKeyContent the new use key content
	 */
	public void setUseKeyContent(boolean useKeyContent) {
		this.useKeyContent = useKeyContent;
	}

	/**
	 * Gets the prvkey content.
	 *
	 * @return the prvkey content
	 */
	public byte[] getPrvkeyContent() {
		return prvkeyContent;
	}

	/**
	 * Sets the prvkey content.
	 *
	 * @param prvkeyContent the new prvkey content
	 */
	public void setPrvkeyContent(byte[] prvkeyContent) {
		this.prvkeyContent = prvkeyContent;
	}

	/**
	 * Gets the pubkey content.
	 *
	 * @return the pubkey content
	 */
	public byte[] getPubkeyContent() {
		return pubkeyContent;
	}

	/**
	 * Sets the pubkey content.
	 *
	 * @param pubkeyContent the new pubkey content
	 */
	public void setPubkeyContent(byte[] pubkeyContent) {
		this.pubkeyContent = pubkeyContent;
	}

	/**
	 * Gets the key pair name.
	 *
	 * @return the key pair name
	 */
	public String getKeyPairName() {
		return keyPairName;
	}

	/**
	 * Sets the key pair name.
	 *
	 * @param keyPairName the new key pair name
	 */
	public void setKeyPairName(String keyPairName) {
		this.keyPairName = keyPairName;
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
	 * Gets the passphrase.
	 *
	 * @return the passphrase
	 */
	public String getPassphrase() {
		return passphrase;
	}

	/**
	 * Sets the passphrase.
	 *
	 * @param passphrase the new passphrase
	 */
	public void setPassphrase(String passphrase) {
		this.passphrase = passphrase;
	}

	/**
	 * Gets the prvkey path.
	 *
	 * @return the prvkey path
	 */
	public String getPrvkeyPath() {
		return prvkeyPath;
	}

	/**
	 * Sets the prvkey path.
	 *
	 * @param prvkeyPath the new prvkey path
	 */
	public void setPrvkeyPath(String prvkeyPath) {
		this.prvkeyPath = prvkeyPath;
	}
	
	/**
	 * Formatting the private id_rsa key.
	 *
	 * @param prvkey the prvkey
	 * @return the string
	 */
	public String formatPrivateKey(String prvkey) {
		
		String[] prvtkeySplit = prvkey.split(" ");
		StringBuilder bld = new StringBuilder();

		for (int i = 0; i < prvtkeySplit.length; i++) {
			if (i == prvtkeySplit.length - 1) {
				bld.append(prvtkeySplit[i]);
			} else {
				if (prvtkeySplit[i].contains("BEGIN") || prvtkeySplit[i].contains("RSA")
						|| prvtkeySplit[i].contains("PRIVATE") || prvtkeySplit[i].contains("END")
						|| prvtkeySplit[i].endsWith(":")) {
					bld.append(prvtkeySplit[i]).append(" ");
				} else {
					bld.append(prvtkeySplit[i]).append("\n");
				}
			}
		}
		return bld.toString();
	}

	/**
	 * Formatting the public id_rsa key.
	 *
	 * @param publicKey the public key
	 * @return the string
	 */
	public String formatPublicKey(String publicKey) {

		if(!publicKey.endsWith("\n")) {
			return publicKey.concat("\n");
		}
		return publicKey;
	}
	
}
