// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.common.SftpProxyType;
import com.boomi.util.StringUtil;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.ProxyHTTP;
import com.jcraft.jsch.ProxySOCKS4;
import com.jcraft.jsch.ProxySOCKS5;

import java.util.logging.Logger;

import static com.boomi.util.NumberUtil.toEnum;

public class ProxyImplementation {
	
	/** The Constant logger. */
	private static final Logger logger = Logger.getLogger(ProxyImplementation.class.getName());

	private ProxyImplementation() {
		//Hide implicit constructor
	}

	/**
	 * Gets the proxy.
	 *
	 * @param conProp the con prop
	 * @return the proxy
	 */
	public static Proxy getProxy(ConnectionProperties conProp) {
		PropertiesUtil sftpProxysettings = conProp.getProperties();
		AtomConfig containerConfig = conProp.getConnectorContext().getConfig();
		String proxyHost = null;
		int proxyPort = 0;
		String proxyPassword = null;
		String proxyUser = null;
		if (conProp.getProperties().isProxyEnabled()) {
			SftpProxyType sftpProxyType = toEnum(SftpProxyType.class, sftpProxysettings.getProxyType());
			if (SftpProxyType.ATOM.equals(sftpProxyType)) {
				proxyHost = containerConfig.getProxyConfig().getProxyHost();
				proxyPort = Integer.valueOf(conProp.getConnectorContext()
						.getConfig().getProxyConfig().getProxyPort());
				proxyUser = containerConfig.getProxyConfig().getProxyUser();
				proxyPassword = containerConfig.getProxyConfig().getProxyPassword();
			} else {
				proxyHost = sftpProxysettings.getProxyHost();
				proxyPort = sftpProxysettings.getProxyPort();
				proxyUser = sftpProxysettings.getProxyUser();
				proxyPassword = sftpProxysettings.getProxyPassword();

			}
		}
		return getProxyImplementation(sftpProxysettings.getProxyType(), proxyHost, proxyPort, proxyUser,
				proxyPassword);

	}
	
	/**
	 * Gets the proxy implementation.
	 *
	 * @param proxyType the proxy type
	 * @param proxyHost the proxy host
	 * @param proxyPort the proxy port
	 * @param proxyUser the proxy user
	 * @param proxyPassword the proxy password
	 * @return the proxy implementation
	 */
	private static Proxy getProxyImplementation(String proxyType, String proxyHost, int proxyPort, String proxyUser,
			String proxyPassword) {
		Proxy proxy = null;
		SftpProxyType sftpProxyType = toEnum(SftpProxyType.class, proxyType);
		if (StringUtil.isNotBlank(proxyHost) && 0 < proxyPort) {
			switch (sftpProxyType) {
			case ATOM:
				//Potential error - fallthrough case
			case HTTP:
				proxy = new ProxyHTTP(proxyHost, proxyPort);
				if (StringUtil.isNotBlank(proxyUser)) {
					((ProxyHTTP) proxy).setUserPasswd(proxyUser, proxyPassword);
				} else {
					logger.warning(
							"Attempt to use proxy without credentials. If your proxy server does not need username and password use proxy type SOCKS4");
				}
				return proxy;

			case SOCKS4:
				proxy = new ProxySOCKS4(proxyHost, proxyPort);
				return proxy;

			case SOCKS5:
				proxy = new ProxySOCKS5(proxyHost, proxyPort);
				if (StringUtil.isNotBlank(proxyUser)) {
					((ProxySOCKS5) proxy).setUserPasswd(proxyUser, proxyPassword);
				} else {
					logger.warning(
							"Attempt to use proxy type SOCKS5 without credentials. If your proxy server does not need username and password use proxy type SOCKS4");
				}
				break;
			default:
				logger.warning("Unsupported proxy type");
				break;
			}
		}
		return proxy;
	}
}
