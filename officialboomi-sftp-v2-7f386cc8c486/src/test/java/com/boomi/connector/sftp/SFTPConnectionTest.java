// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.constants.SFTPTestConstants;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(SFTPClient.class)
public class SFTPConnectionTest {

	@Test
	public void testGetClient() throws JSchException, SftpException {
		testConnection(SFTPTestConstants.TEST_GET_CLIENT, SFTPConstants.USING_PUBLIC_KEY, false);
	}

	@Test
	public void testGetClientUserNamePassword() throws JSchException, SftpException {
		testConnection(SFTPTestConstants.TEST_GET_CLIENT_USER_NAME_PASSWORD, SFTPConstants.USERNAME_AND_PASSWORD, null);
	}
	
	@Test
	public void testGetPathsHandler() throws JSchException, SftpException {
		testConnection(""
				+ "", SFTPConstants.USING_PUBLIC_KEY, false);
	}

	@Test
	public void testIsConnected() throws JSchException, SftpException {
		testConnection(SFTPTestConstants.TEST_GET_CLIENT, SFTPConstants.USING_PUBLIC_KEY, false);
	}

	@Test
	public void testCloseConnection() throws JSchException, SftpException {
		testConnection(SFTPTestConstants.TEST_GET_CLIENT, SFTPConstants.USING_PUBLIC_KEY, false);
	}

	
	@Test
	public void testRenameFile() throws JSchException, SftpException {
		testConnection(SFTPTestConstants.TEST_RENAME_FILE, SFTPConstants.USING_PUBLIC_KEY, false);
	}

	private static void testConnection(String testCase, String authorizationType, Boolean useKeyContent)
			throws JSchException, SftpException {
		try {
			JSch jsch = mock(JSch.class);
			Session session = mock(Session.class);
			ChannelSftp channel = mock(ChannelSftp.class);
			PowerMockito.whenNew(JSch.class).withNoArguments().thenReturn(jsch);
			when(jsch.getSession(any(String.class), any(String.class), any(Integer.class))).thenReturn(session);
			when(session.openChannel(any(String.class))).thenReturn(channel);
			if (testCase.equals(SFTPTestConstants.TEST_RENAME_FILE)) {
				doNothing().when(channel).put(any(InputStream.class), any(String.class));
			}
			Map<String, Object> opProperty = new HashMap<>();
			Map<String, Object> connProperty = new HashMap<>();
			opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
			connProperty.put(SFTPConstants.AUTHORIZATION_TYPE, authorizationType);
			connProperty.put(SFTPConstants.IS_MAX_EXCHANGE, Boolean.FALSE);
			if (null != useKeyContent) {
				connProperty.put(SFTPConstants.USE_KEY_CONTENT, useKeyContent);
			}

			SimpleBrowseContext context = new SimpleBrowseContext(null, null, OperationType.CREATE, connProperty,
					opProperty);
			SFTPConnection conn = new SFTPConnection(context);
			conn.openConnection();
			switch (testCase) {
				case SFTPTestConstants.TEST_GET_CLIENT:
				case SFTPTestConstants.TEST_GET_CLIENT_USER_NAME_PASSWORD:
					conn.getClient();
					break;
				case SFTPTestConstants.TEST_GET_PATHS_HANDLER:
					conn.getPathsHandler();
					break;
				case SFTPTestConstants.TEST_IS_CONNECTED:
					conn.isConnected();
					break;
				case SFTPTestConstants.TEST_CLOSE_CONNECTION:
					conn.closeConnection();
					break;
				case SFTPTestConstants.TEST_RENAME_FILE:
				default:
					break;
			}
		} catch (JSchException | SftpException e) {
			throw e;
		} catch (Exception e) {
			Logger logger = Logger.getLogger(SFTPConnectionTest.class.getName());
			logger.log(Level.INFO, e.getMessage());
		}
	}

}
