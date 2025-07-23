//Copyright (c) 2020 Boomi, Inc.

package com.boomi.connector.sftp.common;


import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.constants.SFTPConstants;

/**
 * The Class PropertiesUtil.
 *
 * @author Omesh Deoli
 * 
 * 
 */
public class PropertiesUtil {

	/** The connection properties. */
	private final PropertyMap connectionProperties;
	

	/**
	 * Instantiates a new properties util.
	 *
	 * @param connectionProperties the connection properties
	 * @param operationProperties the operation properties
	 */
	public PropertiesUtil(PropertyMap connectionProperties) {
		super();
		this.connectionProperties = connectionProperties;
	}

	/**
	 * Gets the hostname.
	 *
	 * @return the hostname
	 */
	public String getHostname() {
		return connectionProperties.getProperty(SFTPConstants.PROPERTY_HOST, "");
	}

	/**
	 * Gets the username.
	 *
	 * @return the username
	 */
	public String getUsername() {
		return connectionProperties.getProperty(SFTPConstants.PROPERTY_USERNAME, "");
	}

	/**
	 * Gets the password.
	 *
	 * @return the password
	 */
	public String getPassword() {
		return connectionProperties.getProperty(SFTPConstants.PROPERTY_PKEY, "");
	}

	/**
	 * Gets the port.
	 *
	 * @return the port
	 */
	public int getPort() {
		return connectionProperties.getLongProperty(SFTPConstants.PROPERTY_PORT, 22L).intValue();
	}

	/**
	 * Gets the auth type.
	 *
	 * @return the auth type
	 */
	public String getAuthType() {
		return connectionProperties.getProperty(SFTPConstants.AUTHORIZATION_TYPE, "");
	}

	/**
	 * Gets the passphrase.
	 *
	 * @return the passphrase
	 */
	public String getpassphrase() {
		return connectionProperties.getProperty(SFTPConstants.KEY_PSWRD, "");
	}

	/**
	 * Gets the prvtkey path.
	 *
	 * @return the prvtkey path
	 */
	public String getprvtkeyPath() {
		return connectionProperties.getProperty(SFTPConstants.KEY_PATH, "");
	}


	/**
	 * Gets the known host entry.
	 *
	 * @return the known host entry
	 */
	public String getKnownHostEntry() {

		return connectionProperties.getProperty(SFTPConstants.HOST_ENTRY, "");
	}

	/**
	 * Checks if is max exchange enabled.
	 *
	 * @return true, if is max exchange enabled
	 */
	public boolean isMaxExchangeEnabled() {

		return connectionProperties.getBooleanProperty(SFTPConstants.IS_MAX_EXCHANGE, false);
	}

	/**
	 * Gets the private key content.
	 *
	 * @return the private key content
	 */
	public String getPrivateKeyContent() {
		return connectionProperties.getProperty(
				SFTPConstants.PRIVATE_KEY_CONTENT, "");
	}

	/**
	 * Gets the public key content.
	 *
	 * @return the public key content
	 */
	public String getPublicKeyContent() {
		return connectionProperties.getProperty(SFTPConstants.PUBLIC_KEY_CONTENT, "");
	}
	
	/**
	 * Gets the key pair name.
	 *
	 * @return the key pair name
	 */
	public String getKeyPairName() {
		return connectionProperties.getProperty(SFTPConstants.KEY_PAIR_NAME, "");
	}
	
	/**
	 * Checks if is use key content enabled.
	 *
	 * @return true, if is use key content enabled
	 */
	public boolean isUseKeyContentEnabled() {
		return connectionProperties.getBooleanProperty(SFTPConstants.USE_KEY_CONTENT, false);
	}
	
	/**
	 * Checks if is proxy enabled.
	 *
	 * @return true, if is proxy enabled
	 */
	public boolean isProxyEnabled() {
		return connectionProperties.getBooleanProperty(SFTPConstants.PROXY_ENABLED, false);
	}
	
	public boolean isPoolingEnabled() {
		return connectionProperties.getBooleanProperty(SFTPConstants.ENABLE_POOLING, false);
	}
	
	/**
	 * Gets the proxy host.
	 *
	 * @return the proxy host
	 */
	public String getProxyHost() {
		return connectionProperties.getProperty(SFTPConstants.PROXY_HOST, "");
	}

	/**
	 * Gets the proxy port.
	 *
	 * @return the proxy port
	 */
	public int getProxyPort() {
		return connectionProperties.getLongProperty(SFTPConstants.PROXY_PORT).intValue();
	}

	/**
	 * Gets the proxy user.
	 *
	 * @return the proxy user
	 */
	public String getProxyUser() {
		return connectionProperties.getProperty(SFTPConstants.PROXY_USERNAME, "");
	}

	/**
	 * Gets the proxy password.
	 *
	 * @return the proxy password
	 */
	public String getProxyPassword() {
		return connectionProperties.getProperty(SFTPConstants.PROXY_PKEY, "");
	}

	/**
	 * Gets the proxy type.
	 *
	 * @return the proxy type
	 */
	public String getProxyType() {
		return connectionProperties.getProperty(SFTPConstants.PROXY_TYPE);
	}
	
}
