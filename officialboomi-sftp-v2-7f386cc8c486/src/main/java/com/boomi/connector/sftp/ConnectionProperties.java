//Copyright (c) 2020 Boomi, Inc.
package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.common.SSHOptions;

/**
 * The Class ConnectionProperties.
 * @author Omesh Deoli
 */
public class ConnectionProperties {
	
	/** The host. */
	private String host;
	
	/** The port. */
	private int port;
	
	/** The auth type. */
	private AuthType authType;
	
	/** The public keyparam. */
	private PublicKeyParam publicKeyparam;
	
	/** The password param. */
	private PasswordParam passwordParam;
	
	/** The ssh options. */
	private final SSHOptions sshOptions;
	
	private final PropertiesUtil properties;
	
	/** The default connection timed out. */
	private int defaultConnectionTimedOut;
	
	/** The default read timed out. */
	private int defaultReadTimedOut;
	
	private ConnectorContext connectorContext;
	
	private long currentDate;
	

	/**
	 * Instantiates a new connection properties.
	 *
	 * @param host the host
	 * @param port the port
	 * @param authType the auth type
	 * @param publicKeyparam the public keyparam
	 * @param properties the properties
	 * @param connectionTimeout the connection timeout
	 * @param readTimeout the read timeout
	 * @param connectorContext 
	 */
	public ConnectionProperties(String host, int port, AuthType authType, PublicKeyParam publicKeyparam,
			PropertiesUtil properties, int connectionTimeout, int readTimeout, ConnectorContext connectorContext) {
		this.host = host;
		this.port = port;
		this.authType = authType;
		this.publicKeyparam = publicKeyparam;
		this.sshOptions = new SSHOptions(properties);
		this.defaultConnectionTimedOut = connectionTimeout;
		this.defaultReadTimedOut = readTimeout;
		this.properties = properties;
		this.connectorContext = connectorContext;
	}
	
	/**
	 * Instantiates a new connection properties.
	 *
	 * @param host the host
	 * @param port the port
	 * @param authType the auth type
	 * @param passwordParam the password param
	 * @param properties the properties
	 * @param connectionTimeout the connection timeout
	 * @param readTimeout the read timeout
	 */
	public ConnectionProperties(String host, int port, AuthType authType, PasswordParam passwordParam,
			PropertiesUtil properties, int connectionTimeout, int readTimeout, ConnectorContext connectorContext) {
		this.host = host;
		this.port = port;
		this.authType = authType;
		this.passwordParam = passwordParam;
		this.sshOptions = new SSHOptions(properties);
		this.defaultConnectionTimedOut = connectionTimeout;
		this.defaultReadTimedOut = readTimeout;
		this.properties = properties;
		this.connectorContext = connectorContext;

	}
	
	/**
	 * Gets the default read timed out.
	 *
	 * @return the default read timed out
	 */
	public int getDefaultReadTimedOut() {
		return defaultReadTimedOut;
	}


	/**
	 * Gets the host.
	 *
	 * @return the host
	 */
	public String getHost() {
		return host;
	}
	
	
	public long getCurrentDate() {
		return currentDate;
	}

	public void setCurrentDate(long currentDate) {
		this.currentDate = currentDate;
	}
	/**
	 * Gets the default connection timed out.
	 *
	 * @return the default connection timed out
	 */
	public int getDefaultConnectionTimedOut() {
		return defaultConnectionTimedOut;
	}

	/**
	 * Gets the port.
	 *
	 * @return the port
	 */
	public int getPort() {
		return port;
	}
	
	/**
	 * Gets the auth.
	 *
	 * @return the auth
	 */
	public AuthType getAuth() {
		return authType;
	}
	
	/**
	 * Gets the pub key param.
	 *
	 * @return the pub key param
	 */
	public PublicKeyParam getPubKeyParam() {
		return publicKeyparam;
	}
	
	/**
	 * Gets the password param.
	 *
	 * @return the password param
	 */
	public PasswordParam getPasswordParam() {
		return passwordParam;
	}
	
	/**
	 * Gets the SSH options.
	 *
	 * @return the SSH options
	 */
	public SSHOptions getSSHOptions() {
		return sshOptions;
	}
	
	public PropertiesUtil getProperties() {
		return properties;
	}

	public ConnectorContext getConnectorContext() {
		return connectorContext;
	}
	
	public String getKey() {
		return getHost()+ getPort() + getProperties().getHostname() + getProperties().getPassword()+ getProperties().getPrivateKeyContent() + getProperties().getProxyHost()+ getProperties().getProxyPassword() + getProperties().getUsername();
	}
	
}
