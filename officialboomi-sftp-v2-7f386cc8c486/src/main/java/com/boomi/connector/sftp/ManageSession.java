// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.util.StringUtil;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.jce.SignatureDSA;
import com.jcraft.jsch.jce.SignatureRSA;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ManageSession {

	private ManageSession() {
		//Hide implicit constructor
	}

	public static Session getSessionWithoutConnectionPooling(ConnectionProperties conProp) {

		Session session = null;
		try {
			JSch jsch = new JSch();
			boolean usepk = false;
			boolean useKnownHosts = false;
			boolean useDHKeySize1024 = true;
			String pkpath = null;
			String pkpassword = null;
			if (conProp.getSSHOptions() != null) {
				usepk = conProp.getSSHOptions().isSshkeyauth();
				pkpath = conProp.getSSHOptions().getSshkeypath();
				pkpassword = conProp.getSSHOptions().getSshkeypassword();
				useKnownHosts = !StringUtil.isBlank(conProp.getSSHOptions().getKnownHostEntry());

				setJschKnownHosts(conProp, jsch, useKnownHosts);

				useDHKeySize1024 = conProp.getSSHOptions().isDhKeySizeMax1024();
			}
			addIdentity(jsch, usepk, pkpath, pkpassword);

			session = getSession(conProp, jsch, useKnownHosts, useDHKeySize1024);

		} catch (JSchException e) {
			catchJschException(conProp, session, e);
		} catch (Exception e) {
			killSession(session);
			throw new ConnectorException(SFTPConstants.ERROR_FAILED_SFTP_SERVER_CONNECTION
					+ SFTPConstants.ERROR_FAILED_SFTP_LOGIN + SFTPConstants.CAUSE + e.getMessage(), e);
		}
		return session;
	}

	/**
	 * @param conProp connection properties
	 * @param jsch java secure channel
	 * @param useKnownHosts use known hosts or not
	 * @throws JSchException
	 */
	private static void setJschKnownHosts(ConnectionProperties conProp, JSch jsch, boolean useKnownHosts)
			throws JSchException, IOException {
		if (useKnownHosts) {
			try (ByteArrayInputStream knownHosts = new ByteArrayInputStream(
					conProp.getSSHOptions().getKnownHostEntry().getBytes(StandardCharsets.UTF_8))) {
				jsch.setKnownHosts(knownHosts);
			}
		}
	}

	/**
	 * Catches Jsch exception and throws appropriate connector exception
	 * @param conProp connection properties
	 * @param session jsch session instance
	 * @param e exception to be processed
	 */
	private static void catchJschException(ConnectionProperties conProp, Session session, JSchException e) {
		if (session == null) {
			throw new ConnectorException(
					SFTPConstants.ERROR_FAILED_CREATING_SESSION + SFTPConstants.CAUSE + e.getMessage(), e);
		}
		if (!session.isConnected()) {
			if (e.getCause() instanceof java.net.UnknownHostException) {
				throw new ConnectorException(
						SFTPConstants.ERROR_FAILED_CONNECTION_TO_HOST + SFTPConstants.CAUSE + e.getMessage(), e);
			}
			if (e.getCause() instanceof java.net.ConnectException) {
				throw new ConnectorException(
						e.getMessage() + " timeout after " + conProp.getDefaultConnectionTimedOut() + " ms ");
			} else {
				throw new ConnectorException(
						SFTPConstants.ERROR_FAILED_SFTP_LOGIN + " after " + conProp.getDefaultConnectionTimedOut()
								+ " ms " + SFTPConstants.CAUSE + "Connection " + e.getMessage());
			}
		} else {
			killSession(session);
			throw new ConnectorException(SFTPConstants.ERROR_FAILED_SFTP_SERVER_CONNECTION
					+ SFTPConstants.ERROR_FAILED_SFTP_LOGIN + SFTPConstants.CAUSE + e.getMessage(), e);
		}
	}

	/**
	 * @param conProp connection properties
	 * @param jsch java secure channel
	 * @param useKnownHosts use known hosts or not
	 * @param useDHKeySize1024 use DHKeySize1024 or not
	 * @return session jsch session instance
	 * @throws JSchException
	 */
	private static Session getSession(ConnectionProperties conProp, JSch jsch, boolean useKnownHosts, boolean useDHKeySize1024) throws JSchException {
		Properties config = new Properties();
		Session session = null;
		if (conProp.getHost() != null) {
			setConfigProperties(config, useKnownHosts, useDHKeySize1024);
			switch (conProp.getAuth()) {
				case PUBLIC_KEY:
					if (!conProp.getPubKeyParam().isUseKeyContentEnabled()) {
						jsch.addIdentity(conProp.getPubKeyParam().getPrvkeyPath(),
								conProp.getPubKeyParam().getPassphrase());
					} else {
						jsch.addIdentity(conProp.getPubKeyParam().getKeyPairName(),
								conProp.getPubKeyParam().getPrvkeyContent(),
								conProp.getPubKeyParam().getPubkeyContent(),
								conProp.getPubKeyParam().getPassphraseContent());

					}
					session = jsch.getSession(conProp.getPubKeyParam().getUser(), conProp.getHost(), conProp.getPort());
					break;
				case PASSWORD:
					session = jsch.getSession(conProp.getPasswordParam().getUser(), conProp.getHost(),
							conProp.getPort());
					session.setPassword(conProp.getPasswordParam().getPassword());
					break;
				default:
					break;
			}
			Proxy proxy = ProxyImplementation.getProxy(conProp);
			if (session != null) {
				if (proxy != null) {
					session.setProxy(proxy);
				}
				session.setConfig(config);
				session.setDaemonThread(true);
				session.setTimeout(conProp.getDefaultConnectionTimedOut());
				session.connect();
				session.setTimeout(conProp.getDefaultReadTimedOut());
			}
		}
		return session;
	}

	/**
	 * @param useKnownHosts used known hosts or not
	 * @param useDHKeySize1024 use DHKeySize1024
	 * @param config configuration properties
	 */
	private static void setConfigProperties(Properties config, boolean useKnownHosts, boolean useDHKeySize1024) {
		config.setProperty(SFTPConstants.PROP_STRICT_HOST_KEY_CHECKING, SFTPConstants.SHKC_NO);
		config.setProperty(SFTPConstants.PREFERRED_AUTHENTICATIONS, SFTPConstants.AUTH_SEQUENCE_FULL);
		config.put(SFTPConstants.KEY_COMP_S2C_ALG, SFTPConstants.COMP_ALGS);
		config.put(SFTPConstants.KEY_COMP_C2S_ALG, SFTPConstants.COMP_ALGS);
		config.put(SFTPConstants.SIGNATURE_DSS_KEY, SignatureDSA.class.getCanonicalName());
		config.put(SFTPConstants.SIGNATURE_RSA_KEY, SignatureRSA.class.getCanonicalName());
		if (useDHKeySize1024) {
			config.put(SFTPConstants.DH_GROUP_EXCHANGE_SHA1, SFTPConstants.CLASS_DHGEX1024);
			config.put(SFTPConstants.DH_GROUP_EXCHANGE_SHA256, SFTPConstants.CLASS_DHGEX256_1024);
			config.put(SFTPConstants.KEX, SFTPConstants.LEGACY_ALGO_LIST);
		}
		if (useKnownHosts) {
			config.put(SFTPConstants.PROP_STRICT_HOST_KEY_CHECKING, SFTPConstants.YES);
		}
	}

	/**
	 * @param jsch java secure channel
	 * @param usepk use private key
	 * @param pkpath private key path
	 * @param pkpassword private key file password
	 */
	private static void addIdentity(JSch jsch, boolean usepk, String pkpath, String pkpassword) {
		if (usepk && !StringUtil.isBlank(pkpath)) {
			try {
				jsch.addIdentity(pkpath, pkpassword);
			} catch (JSchException e) {
				throw new ConnectorException(SFTPConstants.UNABLE_TO_PARSE_SSH_KEY + e.getMessage(), (Throwable) e);
			}
		}
	}

	/**
	 * Kill session.
	 *
	 * @param session the session
	 */
	public static void killSession(Session session) {
		if (session != null && session.isConnected()) {
			try {
				session.disconnect();
			} catch (Exception e) {
				Logger.getLogger(SFTPClient.class.getName()).log(Level.FINE, SFTPConstants.ERROR_DISCONNECTING_SESSION,
						e);
			}
		}
	}
	

}
