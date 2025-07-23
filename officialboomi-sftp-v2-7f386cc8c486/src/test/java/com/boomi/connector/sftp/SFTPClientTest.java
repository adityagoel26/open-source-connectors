// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.sftp;

import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.jcraft.jsch.ChannelSftp;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SFTPClientTest {

    private static final String TESTFILE = "TestFile.txt";
    private static final String DIRECTORYPATH = "path/to/folder";
    private final List<ChannelSftp.LsEntry> _directoryContent = new ArrayList<>();
    private PasswordParam _passwordParam = new PasswordParam("uname", "pass");
    private PropertiesUtil _propertiesUtil = mock(PropertiesUtil.class);
    private PropertyMap _propertyMap = mock(PropertyMap.class);
    private ConnectorContext _connectorContext = mock(ConnectorContext.class);
    private SFTPClient _sftpClient;
    private SFTPClient _sftpClientSpy;
    private ChannelSftp.LsEntry _lsEntry1 = mock(ChannelSftp.LsEntry.class);
    private ChannelSftp.LsEntry _lsEntry2 = mock(ChannelSftp.LsEntry.class);
    private ChannelSftp.LsEntry _lsEntry3 = mock(ChannelSftp.LsEntry.class);

    @Before
    public void init() {

        when(_connectorContext.getConnectionProperties()).thenReturn(_propertyMap);
        when(_propertyMap.getBooleanProperty(anyString())).thenReturn(false);

        when(_propertiesUtil.getKnownHostEntry()).thenReturn("");
        when(_propertiesUtil.getAuthType()).thenReturn("Username and Password");
        when(_propertiesUtil.getprvtkeyPath()).thenReturn("");
        when(_propertiesUtil.getpassphrase()).thenReturn("");
        when(_propertiesUtil.isMaxExchangeEnabled()).thenReturn(false);

        _sftpClient = new SFTPClient("SftpHost", 22, AuthType.PASSWORD, _passwordParam, _propertiesUtil, 120000, 120000,
                _connectorContext);
        _sftpClientSpy = Mockito.spy(_sftpClient);
    }

    @Test
    public void testGetUniqueFileNameWithExtension() {
        String expectedFileName = "TestFile4.txt";

        populateDirectoryContent();

        when(_lsEntry1.getFilename()).thenReturn("TestFile1.txt");
        when(_lsEntry2.getFilename()).thenReturn("TestFile3.txt");
        when(_lsEntry3.getFilename()).thenReturn("TestFile2.txt");

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());
        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, TESTFILE);

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameMoreThanTenFiles() {
        String expectedFileName = "TestFile11.txt";

        populateDirectoryContent();

        when(_lsEntry1.getFilename()).thenReturn("TestFile5.txt");
        when(_lsEntry2.getFilename()).thenReturn("TestFile10.txt");
        when(_lsEntry3.getFilename()).thenReturn("TestFile6.txt");

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());
        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, TESTFILE);

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameMoreThanHundredFiles() {
        String expectedFileName = "TestFile101.txt";

        populateDirectoryContent();

        when(_lsEntry1.getFilename()).thenReturn("TestFile1.txt");
        when(_lsEntry2.getFilename()).thenReturn("TestFile100.txt");
        when(_lsEntry3.getFilename()).thenReturn("TestFile25.txt");

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());
        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, TESTFILE);

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameEmptyDirectory() {
        String expectedFileName = TESTFILE;

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());
        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, TESTFILE);

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameWithAndWithoutExtension() {
        String expectedFileName = "TestFile3";

        populateDirectoryContent();

        when(_lsEntry1.getFilename()).thenReturn("TestFile1");
        when(_lsEntry2.getFilename()).thenReturn("TestFile3.txt");
        when(_lsEntry3.getFilename()).thenReturn("TestFile2");

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());

        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, "TestFile");

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameNumeric() {
        String expectedFileName = "12341.xml";

        _directoryContent.add(_lsEntry1);

        when(_lsEntry1.getFilename()).thenReturn("1234.xml");
        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());

        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, "1234.xml");

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameSpecialChars() {
        String expectedFileName = "Paper>:1.xml";

        _directoryContent.add(_lsEntry2);

        when(_lsEntry2.getFilename()).thenReturn("Paper>:.xml");
        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());

        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, "Paper>:.xml");

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameMultiPeriods() {
        String expectedFileName = "files.in1.csv";

        _directoryContent.add(_lsEntry3);

        when(_lsEntry3.getFilename()).thenReturn("files.in.csv");
        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());

        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, "files.in.csv");

        assertEquals(expectedFileName, uniqueFileName);
    }

    @Test
    public void testGetUniqueFileNameSystemFiles() {
        String expectedFileName = ".ignore1";

        _directoryContent.add(_lsEntry3);

        when(_lsEntry3.getFilename()).thenReturn(".ignore");
        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(anyString());

        String uniqueFileName = _sftpClientSpy.getUniqueFileName(DIRECTORYPATH, ".ignore");

        assertEquals(expectedFileName, uniqueFileName);
    }

    private void populateDirectoryContent() {
        _directoryContent.add(_lsEntry1);
        _directoryContent.add(_lsEntry2);
        _directoryContent.add(_lsEntry3);
    }
}
