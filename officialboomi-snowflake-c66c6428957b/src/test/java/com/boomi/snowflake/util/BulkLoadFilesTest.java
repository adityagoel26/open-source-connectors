// Copyright (c) 2023 Boomi, Inc.
package com.boomi.snowflake.util;

import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.wrappers.BulkLoadFiles;
import com.boomi.util.StringUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BulkLoadFilesTest {

    private static final String WITH_OUT_FILE_FORMAT_NAME =
            " FILE_FORMAT = ( TYPE = 'CSV', COMPRESSION = 'AUTO', SKIP_HEADER = 1 FIELD_DELIMITER = '|')";
    private static final String WITH_FILE_FORMAT_NAME =
            " FILE_FORMAT = ( FORMAT_NAME = 'POV_CSV', SKIP_HEADER = 1 FIELD_DELIMITER = '|')";
    private static final String _fileFormatType = "CSV";
    private static final String _fileFormatName = "POV_CSV";
    private static final String _otherFormatOptions = "SKIP_HEADER = 1 FIELD_DELIMITER = '|'";
    private static final String _copyOptions = "purge";
    private static final String _Compression = "AUTO";
    private static final String _ColumnNames = "TEST";
    private ConnectionProperties connectionProperties;
    private StageHandler stageHandler;
    private static final Logger logger = mock(Logger.class);

    @Before
    public void setUp() {
        connectionProperties = mock(ConnectionProperties.class);
        stageHandler = mock(StageHandler.class);
        when(connectionProperties.getLogger()).thenReturn(logger);
    }

    /**
     * This method tests the If condition for without file format name
     */
    @Test
    public void testGetFileFormat() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(connectionProperties.getFileFormatType()).thenReturn(_fileFormatType);
        when(connectionProperties.getFileFormatName()).thenReturn(StringUtil.EMPTY_STRING);
        when(connectionProperties.getOtherFormatOptions()).thenReturn(_otherFormatOptions);
        when(connectionProperties.getCopyOptions()).thenReturn(_copyOptions);
        when(connectionProperties.getCompression()).thenReturn(_Compression);
        when(connectionProperties.getColumnNames()).thenReturn(StringUtil.EMPTY_STRING);
        BulkLoadFiles bulkLoadFiles = new BulkLoadFiles(connectionProperties, stageHandler);
        String fileFormat = bulkLoadFiles.getFileFormat();
        Assert.assertEquals(WITH_OUT_FILE_FORMAT_NAME, fileFormat);
    }

    /**
     * This method tests the If condition for with file format name
     */
    @Test
    public void testGetFileFormat2() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(connectionProperties.getFileFormatType()).thenReturn(_fileFormatType);
        when(connectionProperties.getFileFormatName()).thenReturn(_fileFormatName);
        when(connectionProperties.getOtherFormatOptions()).thenReturn(_otherFormatOptions);
        when(connectionProperties.getCopyOptions()).thenReturn(_copyOptions);
        when(connectionProperties.getColumnNames()).thenReturn(_ColumnNames);
        BulkLoadFiles bulkLoadFiles = new BulkLoadFiles(connectionProperties, stageHandler);
        String fileFormat = bulkLoadFiles.getFileFormat();
        Assert.assertEquals(WITH_FILE_FORMAT_NAME, fileFormat);
    }

    /**
     * This method tests the If condition for empty file format type
     */
    @Test
    public void testGetFileFormat3() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        when(connectionProperties.getFileFormatType()).thenReturn(StringUtil.EMPTY_STRING);
        when(connectionProperties.getFileFormatName()).thenReturn(_fileFormatName);
        when(connectionProperties.getOtherFormatOptions()).thenReturn(_otherFormatOptions);
        when(connectionProperties.getCopyOptions()).thenReturn(StringUtil.EMPTY_STRING);
        when(connectionProperties.getColumnNames()).thenReturn(StringUtil.EMPTY_STRING);
        BulkLoadFiles bulkLoadFiles = new BulkLoadFiles(connectionProperties, stageHandler);
        String fileFormat = bulkLoadFiles.getFileFormat();
        Assert.assertEquals(StringUtil.EMPTY_STRING, fileFormat);
    }
}