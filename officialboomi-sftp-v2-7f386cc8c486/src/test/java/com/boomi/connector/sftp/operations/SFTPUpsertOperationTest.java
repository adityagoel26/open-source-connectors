// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnector;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.constants.SFTPTestConstants;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutil.SimpleOperationContext;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(SFTPClient.class)
public class SFTPUpsertOperationTest {

	@Test
	public void testExecuteUpdateUpdateRequestOperationResponse()
			throws Exception {
	
		executeUpdateRequest(true, false);
	}
	
	@Test
	public void testExecuteUpdateUpdateRequestOperationResponseAppendDisabled()
			throws Exception {
	
		executeUpdateRequest(false, false);
	}

	@Test
	public void testExecuteUpdateUpdateRequestOperationResponseException() throws Exception {

		executeUpdateRequest(true, true);
	}

	private static void executeUpdateRequest(boolean append, boolean throwException) throws Exception {
		JSch jsch = mock(JSch.class);
		Session session = mock(Session.class);
		ChannelSftp channel = mock(ChannelSftp.class);

		PowerMockito.whenNew(JSch.class).withNoArguments().thenReturn(jsch);

		when(jsch.getSession(any(String.class), any(String.class), any(Integer.class))).thenReturn(session);
		if(throwException){
			when(session.openChannel(any(String.class))).thenThrow(new RuntimeException());
		} else {
			when(session.openChannel(any(String.class))).thenReturn(channel);
		}

		Vector<Object> vect = new Vector<Object>();
		when(channel.ls(any(String.class))).thenReturn(vect);
		SftpATTRS attrs = mock(SftpATTRS.class);
		attrs.setSIZE(1);

		when(channel.lstat(any(String.class))).thenReturn(attrs);
		Map<String, Object> opProperty = new HashMap<>();
		Map<String, Object> connProperty = new HashMap<>();
		opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
		connProperty.put(SFTPConstants.AUTHORIZATION_TYPE, SFTPConstants.USING_PUBLIC_KEY);
		connProperty.put(SFTPConstants.IS_MAX_EXCHANGE, Boolean.FALSE);
		connProperty.put(SFTPConstants.USE_KEY_CONTENT, false);
		opProperty.put(SFTPConstants.INCLUDE_ALL, Boolean.TRUE);
		opProperty.put(SFTPConstants.APPEND, append);
		opProperty.put(SFTPConstants.CREATE_DIR, Boolean.TRUE);
		new SimpleBrowseContext(null, null, OperationType.CREATE, connProperty,
				opProperty);
		new SimpleOperationContext(null, null, OperationType.CREATE, null, null,
				null, null);
		SFTPConnector connector = new SFTPConnector();

		ConnectorTester tester = new ConnectorTester(connector);
		Map<ObjectDefinitionRole, String> cookie = new HashMap<>();
		cookie.put(ObjectDefinitionRole.OUTPUT, "true");
		tester.setOperationContext(OperationType.UPSERT, connProperty, opProperty, "File", cookie);
		tester.setBrowseContext(OperationType.UPSERT, connProperty, opProperty);
		List<InputStream> streamList = new ArrayList<>();
		streamList.add(new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)));
		Map<String, String> dynamicProperty = new HashMap<>();
		dynamicProperty.put(SFTPConstants.PROPERTY_FILENAME, SFTPTestConstants.FILE1_TXT);
		SimpleTrackedData trackedData = new SimpleTrackedData(1, new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)),
				null, dynamicProperty);
		List<SimpleTrackedData> listTrackData = new ArrayList<>();
		listTrackData.add(trackedData);
		tester.executeUpsertOperationWithTrackedData(listTrackData);
	}
}
