// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.sftp;

import com.boomi.connector.api.OperationType;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.testutil.SimpleBrowseContext;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import static com.boomi.connector.sftp.AuthType.PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author aishwaryjain.
 */
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
@PrepareForTest(value = { SFTPClient.class, ManageSession.class })
public class SFTPClientConnectionPoolingEnabledTest {

    JSch _jSch;
    SFTPClient _sftpClient;
    Session _session;
    PropertiesUtil _propertiesUtil;
    ChannelSftp _channel;
    Map<String, Object> _operationProperty;
    Map<String, Object> _connectionProperty;
    PasswordParam _passwordParam;
    SimpleBrowseContext _context;
    String _host;
    int _port;
    AuthType _authType;
    int _connectionTimeout = 0;
    int _readTimeout = 0;

    @Before
    public void setUp() throws Exception {

        _jSch = mock(JSch.class);
        _session = PowerMockito.mock(Session.class);
        _channel = mock(ChannelSftp.class);
        _propertiesUtil = mock(PropertiesUtil.class);
        _operationProperty = new HashMap<>();
        _connectionProperty = new HashMap<>();
        _host = "ftp.boomi.com";
        _port = 22;
        _authType = PASSWORD;

        PowerMockito.whenNew(JSch.class).withNoArguments().thenReturn(_jSch);
        when(_jSch.getSession(any(String.class), any(String.class), any(Integer.class))).thenReturn(_session);
        when(_session.openChannel(any(String.class))).thenReturn(_channel);
        _passwordParam = new PasswordParam("", "");
        when(_propertiesUtil.isPoolingEnabled()).thenReturn(true);
        when(_propertiesUtil.getAuthType()).thenReturn("");
        _context = new SimpleBrowseContext(null, null, OperationType.CREATE, _connectionProperty, _operationProperty);

    }

    @Test
    public void testGetSessionIsConnectedTrue()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        PowerMockito.when(_session.isConnected()).thenReturn(true);
        buildSftpClientObject();
        assertNotNull(_sftpClient);
        assertEquals(_session.isConnected(),true);
    }

    @Test
    public void testGetSessionIsConnectedFalse()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        buildSftpClientObject();
        assertNotNull(_sftpClient);
        assertEquals(_session.isConnected(),false);
    }

    @Test(expected = Exception.class)
    public void testGetSessionFailed()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        _host=null;
        buildSftpClientObject();
    }
    @java.lang.SuppressWarnings("java:S3011")
    private void buildSftpClientObject()
            throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {

        _sftpClient = new SFTPClient(_host, _port, _authType, _passwordParam, _propertiesUtil, _connectionTimeout,
                _readTimeout, _context);
        Method method = _sftpClient.getClass().getDeclaredMethod("getSession", null);
        method.setAccessible(true);
        method.invoke(_sftpClient, null);
    }
}
