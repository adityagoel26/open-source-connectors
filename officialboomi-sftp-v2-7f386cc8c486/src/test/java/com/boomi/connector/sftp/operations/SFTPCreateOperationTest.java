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
import com.boomi.util.StringUtil;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;

import org.junit.Test;
import org.junit.runner.RunWith;
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
public class SFTPCreateOperationTest {

	@Test
	public void testSFTPCreateOperation() throws Exception {
		sftpCreateOperation("testSFTPCreateOperation", null, SFTPTestConstants.FILE1_TXT, null, null, false);
	}

	@Test
	public void testSFTPCreateOperationFileDoesntExist()
			throws Exception {
		sftpCreateOperation("testSFTPCreateOperationFileDoesntExist", "FORCE_UNIQUE_NAMES", SFTPTestConstants.FILE1_TXT, null, null, false);
	}

	@Test
	public void testSFTPCreateOperationFileNameNotProvided()
			throws Exception {
		sftpCreateOperation("testSFTPCreateOperationFileDoesntExist", "FORCE_UNIQUE_NAMES", null, null, null, false);
	}

	@Test
	public void testSFTPCreateOperationAPPEND() throws Exception {
		sftpCreateOperation("testSFTPCreateOperationAPPEND", "APPEND", SFTPTestConstants.FILE1_TXT, null, null, false);
	}
	
	@Test
	public void testSFTPCreateOperationOVERWRITE() throws Exception {
		sftpCreateOperation("testSFTPCreateOperationOVERWRITE", SFTPTestConstants.OVERWRITE, SFTPTestConstants.FILE1_TXT, "abc", null, false);
	}



	@Test
	public void testSFTPCreateOperationOVERWRITEtempExtdisabled()
			throws Exception {
		sftpCreateOperation("testSFTPCreateOperationOVERWRITEtempExtdisabled", SFTPTestConstants.OVERWRITE, SFTPTestConstants.FILE1_TXT, null, "/abc", false);
	}
 
	@Test
	public void testSFTPCreateOperationOVERWRITENoCookie()
			throws Exception {
		sftpCreateOperation("testSFTPCreateOperationOVERWRITENoCookie", SFTPTestConstants.OVERWRITE, SFTPTestConstants.FILE1_TXT, null, "/abc", true);
	}
	
	@Test
	public void testSFTPCreateOperationOVERWRITEEmptyCookie()
			throws Exception {
		sftpCreateOperation("testSFTPCreateOperationOVERWRITENoCookie", SFTPTestConstants.OVERWRITE, SFTPTestConstants.FILE1_TXT, null, "/abc", true);
	}

	private static void sftpCreateOperation(String testCase, String actionIfFileExists, String fileName, String tempFileName, String stagingDirectory, boolean nullCookie)
			throws Exception {
		ChannelSftp channel = SFTPGetOperationTest.getChannelSftp();
		switch (testCase){
			case "testSFTPCreateOperation":
				break;
			default:
				new SftpException(2, SFTPTestConstants.FILE_EXISTS);
				Vector<Object> vect = new Vector<Object>();
				when(channel.ls(any(String.class))).thenReturn(vect);
				SftpATTRS attrs = mock(SftpATTRS.class);
				attrs.setSIZE(1);
				when(channel.lstat(any(String.class))).thenReturn(attrs);
		}

		Map<String, Object> opProperty = new HashMap<>();
		Map<String, Object> connProperty = new HashMap<>();
		opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
		if(StringUtil.isNotBlank(actionIfFileExists)){
			opProperty.put(SFTPConstants.OPERATION_PROP_ACTION_IF_FILE_EXISTS, actionIfFileExists);
		}
		if(StringUtil.isNotBlank(tempFileName)){
			opProperty.put(SFTPConstants.PROPERTY_TEMP_FILE_NAME, tempFileName);
		}
		if(StringUtil.isNotBlank(stagingDirectory)){
			opProperty.put(SFTPConstants.PROPERTY_STAGING_DIRECTORY, stagingDirectory);
		}
		connProperty.put(SFTPConstants.AUTHORIZATION_TYPE, SFTPConstants.USING_PUBLIC_KEY);
		connProperty.put(SFTPConstants.IS_MAX_EXCHANGE, Boolean.FALSE);
		connProperty.put(SFTPConstants.USE_KEY_CONTENT, false);
		new SimpleBrowseContext(null, null, OperationType.CREATE, connProperty,
				opProperty);
		new SimpleOperationContext(null, null, OperationType.CREATE, null, null,
				null, null);
		SFTPConnector connector = new SFTPConnector();

		ConnectorTester tester = new ConnectorTester(connector);
		Map<ObjectDefinitionRole, String> cookie = new HashMap<>();
		cookie.put(ObjectDefinitionRole.OUTPUT, "true");
		if(nullCookie){
			tester.setOperationContext(OperationType.CREATE, connProperty, opProperty, "File", null);
		} else {
			tester.setOperationContext(OperationType.CREATE, connProperty, opProperty, "File", cookie);
		}
		tester.setBrowseContext(OperationType.CREATE, connProperty, opProperty);
		List<InputStream> streamList = new ArrayList<>();
		streamList.add(new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)));
		Map<String, String> dynamicProperty = new HashMap<>();
		if(StringUtil.isBlank(fileName)){
			dynamicProperty.put(SFTPConstants.PROPERTY_FILENAME, fileName);
		}
		SimpleTrackedData trackedData = new SimpleTrackedData(1, new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)),
				null, dynamicProperty);
		List<SimpleTrackedData> listTrackData = new ArrayList<>();
		listTrackData.add(trackedData);
		tester.executeCreateOperationWithTrackedData(listTrackData);
	}
}
