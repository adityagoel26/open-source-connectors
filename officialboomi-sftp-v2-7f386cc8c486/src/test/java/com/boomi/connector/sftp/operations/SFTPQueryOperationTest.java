// Copyright (c) 2022 Boomi, Inc.

package com.boomi.connector.sftp.operations;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.QueryFilter;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnector;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.constants.SFTPTestConstants;
import com.boomi.connector.sftp.exception.NoSuchFileFoundException;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.boomi.connector.testutil.ConnectorTester;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.boomi.connector.testutil.SimpleOperationContext;
import com.boomi.connector.testutil.SimpleTrackedData;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.ChannelSftp.LsEntry;
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
public class SFTPQueryOperationTest {

	@Test
	public void testExecuteQueryQueryRequestOperationResponse()
			throws Exception {
		executeQueryRequest("QUERY");
	}

	@Test
	public void testExecuteQueryQueryRequestOperationResponseInvalidQueryType()
			throws Exception {
		executeQueryRequest("qwieqw");
	}
	
	
	@Test
	public void testExecuteQueryQueryRequestOperationResponseLIST()
			throws Exception {
		executeQueryRequest("LIST");
	}
	
	@Test
	public void testExecuteQueryQueryRequestOperationResponseQUERY()
			throws Exception {
		executeQueryRequest("LIST");
	}

	private static void executeQueryRequest(String operationType)
			throws Exception {
		JSch jsch = mock(JSch.class);
		Session session = mock(Session.class);
		ChannelSftp channel = mock(ChannelSftp.class);

		PowerMockito.whenNew(JSch.class).withNoArguments().thenReturn(jsch);

		when(jsch.getSession(any(String.class), any(String.class), any(Integer.class))).thenReturn(session);
		when(session.openChannel(any(String.class))).thenReturn(channel);
		Map<String, Object> opProperty = new HashMap<>();
		Map<String, Object> connProperty = new HashMap<>();
		opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
		opProperty.put(SFTPConstants.COUNT, 3L);
		connProperty.put(SFTPConstants.AUTHORIZATION_TYPE, SFTPConstants.USING_PUBLIC_KEY);
		connProperty.put(SFTPConstants.IS_MAX_EXCHANGE, Boolean.FALSE);
		connProperty.put(SFTPConstants.REMOTE_DIRECTORY, "/abc");
		connProperty.put(SFTPConstants.USE_KEY_CONTENT, false);
		LsEntry entry = mock(LsEntry.class);
		new NoSuchFileFoundException("abc");
		new SFTPSdkException("error", new Throwable());
		Vector<Object> vect = new Vector<Object>();
		vect.add(entry);
		when(channel.ls(any(String.class))).thenReturn(vect);
		SftpATTRS attrs = mock(SftpATTRS.class);
		attrs.setSIZE(1);
		when(entry.getAttrs()).thenReturn(attrs);
		when(attrs.isLink()).thenReturn(Boolean.FALSE);
		when(entry.getFilename()).thenReturn(SFTPTestConstants.MOCK_FILE_NAME);

		when(channel.lstat(any(String.class))).thenReturn(attrs);
		SFTPConnector connector = new SFTPConnector();
		new SimpleOperationContext(null, connector, OperationType.QUERY,
				connProperty, opProperty, null, null);
		QueryOpContext qopcontext = new QueryOpContext(null, connector, OperationType.QUERY, connProperty, opProperty,
				null, null, operationType);

		SimpleBrowseContext context = new SimpleBrowseContext(null, connector, OperationType.QUERY, "LIST",
				connProperty, opProperty);
		ConnectorTester tester = new ConnectorTester(connector);

		Map<ObjectDefinitionRole, String> cookie = new HashMap<>();
		cookie.put(ObjectDefinitionRole.OUTPUT, "true");

		List<InputStream> streamList = new ArrayList<>();
		streamList.add(new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)));

		Map<String, String> dynamicProperty = new HashMap<>();
		dynamicProperty.put(SFTPConstants.PROPERTY_FILENAME, SFTPTestConstants.FILE1_TXT);
		SimpleTrackedData trackedData = new SimpleTrackedData(1, new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)),
				null, dynamicProperty);
		List<SimpleTrackedData> listTrackData = new ArrayList<>();
		listTrackData.add(trackedData);

		tester.setBrowseContext(context);
		tester.setOperationContext(qopcontext);

		QueryFilter filter = new QueryFilter();
		tester.executeQueryOperation(filter);
	}
}
