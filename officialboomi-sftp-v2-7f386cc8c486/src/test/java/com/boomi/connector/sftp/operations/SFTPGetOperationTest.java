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
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(SFTPClient.class)
public class SFTPGetOperationTest {

    @Test
    public void testSFTPGetOperation() throws Exception {
        sftpGetOperation(Boolean.TRUE, Boolean.FALSE, "abc", null, SFTPTestConstants.TEST_SFTP_GET_OPERATION);
    }

    @Test
    public void testSFTPGetOperationDeleteDisabled() throws Exception {
        sftpGetOperation(Boolean.FALSE, Boolean.FALSE, "abc", null,
                SFTPTestConstants.TEST_SFTP_GET_OPERATION_DELETE_DISABLED);
    }

	@Test
    public void testSFTPGetOperationFailAfterDelete() throws Exception {
        sftpGetOperation(Boolean.TRUE, Boolean.TRUE, "abc", null,
                SFTPTestConstants.TEST_SFTP_GET_OPERATION_FAIL_AFTER_DELETE);
    }


	@Test
    public void testSFTPGetOperationDeleteFailed() throws Exception {
        sftpGetOperation(Boolean.TRUE, Boolean.TRUE, "abc", null, SFTPTestConstants.TEST_SFTP_GET_OPERATION_DELETE_FAILED);
    }
	@Test
    public void testSFTPGetOperationBlankFileNAme() throws Exception {
        sftpGetOperation(Boolean.TRUE, Boolean.FALSE, "", null, SFTPTestConstants.TEST_SFTP_GET_OPERATION_BLANK_FILE_N_AME);
    }
	
	
	@Test
    public void testSFTPGetOperationFileNotFound() throws Exception {
        sftpGetOperation(Boolean.TRUE, Boolean.FALSE, "abcd", "abcde",
                SFTPTestConstants.TEST_SFTP_GET_OPERATION_FILE_NOT_FOUND);
    }

    private static void sftpGetOperation(Boolean deleteAfterProperty, Boolean failDeleteAfterProperty, String fileName, String remoteDirectoryProperty, String testCase)
            throws Exception {
        ChannelSftp channel = getChannelSftp();
        switch (testCase) {
            case SFTPTestConstants.TEST_SFTP_GET_OPERATION_FAIL_AFTER_DELETE:
                doThrow(new SftpException(2,SFTPTestConstants.ERROR_OCCURRED))
                        .when(channel).rm((any(String.class)));
                break;
            case SFTPTestConstants.TEST_SFTP_GET_OPERATION_DELETE_FAILED:
                doThrow(new SftpException(3,SFTPTestConstants.ERROR_OCCURRED))
                        .when(channel).rm((any(String.class)));
                break;
            case SFTPTestConstants.TEST_SFTP_GET_OPERATION_FILE_NOT_FOUND:
                doThrow(new SftpException(2,SFTPTestConstants.ERROR_OCCURRED))
                        .when(channel).stat("abcde");
                break;
            default:
                break;
        }

        Map<String, Object> opProperty = new HashMap<>();
        Map<String, Object> connProperty = new HashMap<>();
        opProperty.put(SFTPConstants.PROPERTY_INCLUDE_METADATA, Boolean.TRUE);
        opProperty.put(SFTPConstants.PROPERTY_DELETE_AFTER, deleteAfterProperty);
        if (failDeleteAfterProperty) {
            opProperty.put(SFTPConstants.PROPERTY_FAIL_DELETE_AFTER, Boolean.TRUE);
        }
        if(StringUtil.isNotBlank(remoteDirectoryProperty)){
            opProperty.put(SFTPConstants.PROPERTY_REMOTE_DIRECTORY, remoteDirectoryProperty);
        }
        connProperty.put(SFTPConstants.AUTHORIZATION_TYPE, SFTPConstants.USING_PUBLIC_KEY);
        connProperty.put(SFTPConstants.IS_MAX_EXCHANGE, Boolean.FALSE);
        connProperty.put(SFTPConstants.USE_KEY_CONTENT, false);
        new SimpleBrowseContext(null, null, OperationType.GET, connProperty,
                opProperty);

        new SimpleOperationContext(null, null, OperationType.GET, null, null,
                null, null);
        SFTPConnector connector = new SFTPConnector();

        ConnectorTester tester = new ConnectorTester(connector);
        Map<ObjectDefinitionRole, String> cookie = new HashMap<>();
        cookie.put(ObjectDefinitionRole.OUTPUT, "true");
        tester.setOperationContext(OperationType.GET, connProperty, opProperty, "File", cookie);
        tester.setBrowseContext(OperationType.GET, connProperty, opProperty);
        List<InputStream> streamList = new ArrayList<>();
        streamList.add(new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)));

        Map<String, String> dynamicProperty = new HashMap<>();
        dynamicProperty.put(SFTPConstants.PROPERTY_FILENAME, SFTPTestConstants.FILE1_TXT);

        new SimpleTrackedData(1, new ByteArrayInputStream("abc".getBytes(SFTPConstants.UTF_8)),
                null, dynamicProperty);

        tester.executeGetOperation(fileName);
    }

    static ChannelSftp getChannelSftp() throws Exception {
        JSch jsch = mock(JSch.class);
        Session session = mock(Session.class);
        ChannelSftp channel = mock(ChannelSftp.class);

        PowerMockito.whenNew(JSch.class).withNoArguments().thenReturn(jsch);

        when(jsch.getSession(any(String.class), any(String.class), any(Integer.class))).thenReturn(session);
        when(session.openChannel(any(String.class))).thenReturn(channel);
        return channel;
    }
}
