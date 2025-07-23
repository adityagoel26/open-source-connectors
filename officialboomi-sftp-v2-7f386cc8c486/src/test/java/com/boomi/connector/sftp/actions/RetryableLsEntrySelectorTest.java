// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.sftp.actions;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ConnectorContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.sftp.AuthType;
import com.boomi.connector.sftp.ConnectionProperties;
import com.boomi.connector.sftp.CustomLsEntrySelector;
import com.boomi.connector.sftp.PasswordParam;
import com.boomi.connector.sftp.SFTPClient;
import com.boomi.connector.sftp.SFTPConnection;
import com.boomi.connector.sftp.common.PropertiesUtil;
import com.boomi.connector.sftp.constants.SFTPConstants;
import com.boomi.connector.sftp.exception.FileNameNotSupportedException;
import com.boomi.connector.sftp.exception.SFTPSdkException;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.nio.file.InvalidPathException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class RetryableLsEntrySelectorTest {

    private static final String DIRECTORY_PATH = "/home/qaboomisvc/Ben/Query";
    private final BrowseContext _browseContext = mock(BrowseContext.class);
    private final List<ChannelSftp.LsEntry> _directoryContent = new ArrayList<>();
    private final List<ChannelSftp.LsEntry> _directoryContentEmpty = new ArrayList<>();

    private final TrackedData trackedData = mock(TrackedData.class);
    private final CustomLsEntrySelector _lsEntrySelector = mock(CustomLsEntrySelector.class);
    private final PropertiesUtil _propertiesUtil = mock(PropertiesUtil.class);
    private final ConnectorContext _connectorContext = mock(ConnectorContext.class);
    private final ConnectionProperties _connectionProperties = mock(ConnectionProperties.class);
    private final PropertyMap _propertyMap = mock(PropertyMap.class);
    private final ChannelSftp.LsEntry _listEntry1 = mock(ChannelSftp.LsEntry.class);
    private final ChannelSftp.LsEntry _listEntry2 = mock(ChannelSftp.LsEntry.class);
    private final ChannelSftp.LsEntry _listEntry3 = mock(ChannelSftp.LsEntry.class);
    private final Logger _logger = mock(Logger.class);
    private final PasswordParam _passwordParam = new PasswordParam("uname", "pass");
    private RetryableLsEntrySelector _retryableLsEntrySelector;
    private SFTPClient _sftpClient;

    @InjectMocks
    private SFTPClient _sftpClientSpy;
    @InjectMocks
    private SFTPConnection _sftpConnection;

    @Mock
    private ChannelSftp _channelSftp;

    @Before
    public void init() {
        when(_browseContext.getConnectionProperties()).thenReturn(_propertyMap);
        when(_propertyMap.getProperty(SFTPConstants.REMOTE_DIRECTORY)).thenReturn(DIRECTORY_PATH);
        _sftpConnection = new SFTPConnection(_browseContext);
        when(_connectorContext.getConnectionProperties()).thenReturn(_propertyMap);
        when(_propertyMap.getBooleanProperty(SFTPConstants.ENABLE_POOLING, false)).thenReturn(false);
        when(_propertiesUtil.getKnownHostEntry()).thenReturn("");
        when(_propertiesUtil.getAuthType()).thenReturn("Username and Password");
        when(_propertiesUtil.getprvtkeyPath()).thenReturn("");
        when(_propertiesUtil.getpassphrase()).thenReturn("");
        when(_propertiesUtil.isMaxExchangeEnabled()).thenReturn(false);

        when(trackedData.getLogger()).thenReturn(_logger);

        _sftpClient = new SFTPClient("SftpHost", 22, AuthType.PASSWORD, _passwordParam, _propertiesUtil, 120000, 120000,
                _connectorContext);
        _sftpClientSpy = spy(_sftpClient);

        when(_connectionProperties.getProperties()).thenReturn(_propertiesUtil);
        when(_connectionProperties.getProperties().isPoolingEnabled()).thenReturn(true);

        _directoryContent.add(_listEntry1);
        _directoryContent.add(_listEntry2);
        _directoryContent.add(_listEntry3);

        when(_listEntry1.getFilename()).thenReturn("Test:File.txt");
        when(_listEntry2.getFilename()).thenReturn("Test*File.txt");
        when(_listEntry3.getFilename()).thenReturn("TestFile2.txt");

        Mockito.doReturn(_directoryContent).when(_sftpClientSpy).listDirectoryContent(DIRECTORY_PATH);

        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testFileFormatExceptionWhenInvalidFileNames() throws SftpException {

        SftpException exception = new SftpException(12, "Invalid filenames",
                new InvalidPathException("Invalid name", "Illegal char <*> at index 40: /home/qaboomisvc/Ben/Query/Test:File.txt"));

        _retryableLsEntrySelector = new RetryableLsEntrySelector(_sftpConnection, DIRECTORY_PATH, trackedData,
                _lsEntrySelector);

        doThrow(exception).when(_channelSftp).ls(DIRECTORY_PATH, _lsEntrySelector);

        String exceptionMessage = MessageFormat.format(
                "Error getting directory contents at path ''{0}'' Cause: Illegal char <*> at index 40: "
                        + "/home/qaboomisvc/Ben/Query/Test:File.txt: Invalid name\n\n"
                        + "List of files with non-permissible characters in the filename: \n[Test:File.txt, Test*File"
                        + ".txt]", DIRECTORY_PATH);

        FileNameNotSupportedException fileNameNotSupportedException = new FileNameNotSupportedException(
                exceptionMessage);
        String message = MessageFormat.format(
                "Failed to fetch one or more documents due to non-permissible characters in the filename. \n{0}",
                fileNameNotSupportedException.getMessage());

        _retryableLsEntrySelector.execute();

        Mockito.verify(_logger).log(Level.WARNING, message, fileNameNotSupportedException);
    }

    @Test
    public void testSftpSdkExceptionWhenNoInvalidFiles() throws SftpException {

        Mockito.doReturn(_directoryContentEmpty).when(_sftpClientSpy).listDirectoryContent(DIRECTORY_PATH);
        when(_lsEntrySelector.isReconnectFailed()).thenReturn(false);

        SftpException exception = new SftpException(12, "Connection failed",
                new Throwable("The connection with the remote server failed."));

        _retryableLsEntrySelector = new RetryableLsEntrySelector(_sftpConnection, DIRECTORY_PATH, trackedData,
                _lsEntrySelector);

        doThrow(exception).when(_channelSftp).ls(DIRECTORY_PATH, _lsEntrySelector);

        String exceptionMessage = MessageFormat.format(
                "Error getting directory contents at path ''{0}'' Cause: The connection with "
                        + "the remote server failed.", DIRECTORY_PATH);

        SFTPSdkException sftpSdkException = new SFTPSdkException(exceptionMessage);

        String message = "Connection successfully re-established but failed to fetch one or more documents";

        _retryableLsEntrySelector.execute();

        verify(_logger).log(Level.WARNING, message, sftpSdkException);
    }

    @Test
    public void testSftpSdkExceptionWhenReconnectFailed() throws SftpException {

        Mockito.doReturn(_directoryContentEmpty).when(_sftpClientSpy).listDirectoryContent(DIRECTORY_PATH);
        when(_lsEntrySelector.isReconnectFailed()).thenReturn(true);

        SftpException exception = new SftpException(12, "Failed to fetch documents",
                new Throwable("Failed to fetch one or more documents."));

        _retryableLsEntrySelector = new RetryableLsEntrySelector(_sftpConnection, DIRECTORY_PATH, trackedData,
                _lsEntrySelector);

        doThrow(exception).when(_channelSftp).ls(DIRECTORY_PATH, _lsEntrySelector);

        String exceptionMessage = MessageFormat.format(
                "Error getting directory contents at path ''{0}'' Cause: Failed to fetch " + "one or more documents.",
                DIRECTORY_PATH);
        SFTPSdkException sftpSdkException = new SFTPSdkException(exceptionMessage);

        String message =
                "Failed to re-establish connectivity with remote server after maximum allowed attempts. Could not "
                        + "fetch all the documents";

        _retryableLsEntrySelector.execute();

        verify(_logger).log(Level.WARNING, message, sftpSdkException);
    }
}
