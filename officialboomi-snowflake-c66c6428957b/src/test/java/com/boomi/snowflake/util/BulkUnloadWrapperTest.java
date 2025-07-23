// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.util;

import com.boomi.snowflake.stages.StageHandler;
import com.boomi.snowflake.wrappers.BulkUnloadWrapper;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.logging.Logger;

import org.mockito.Mockito;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ BulkUnloadWrapper.class })
public class BulkUnloadWrapperTest {

    BulkUnloadWrapper bulkUnloadWrapper = Mockito.mock(BulkUnloadWrapper.class);

    /**
     * Method for testing the getFile method in BulkUnloadWrapper class when the file path is null.
     *
     * @throws Exception if an error occurs during the test
     */
    @Test
    public void getFileTest() throws Exception {
        Logger log = Mockito.mock(Logger.class);
        Field logger = Whitebox.getField(BulkUnloadWrapper.class, "_processLogger");
        Field filesPath = Whitebox.getField(BulkUnloadWrapper.class, "_filesPath");
        ArrayList<String> path = new ArrayList<>();
        filesPath.set(bulkUnloadWrapper, path);
        logger.set(bulkUnloadWrapper, log);
        InputStream inputStream = Whitebox.invokeMethod(bulkUnloadWrapper, "getFile");
        Assert.assertNull(inputStream);
    }

    /**
     * Method for testing the getFile method in BulkUnloadWrapper class when the file path is not null.
     *
     * @throws Exception if an error occurs during the test
     */
    @Test
    public void getFileNonNullTest() throws Exception {
        Logger log = Mockito.mock(Logger.class);
        Field logger = Whitebox.getField(BulkUnloadWrapper.class, "_processLogger");
        Field filesPath = Whitebox.getField(BulkUnloadWrapper.class, "_filesPath");
        ArrayList<String> path = new ArrayList<>();
        path.add("storage/path/to/file");
        filesPath.set(bulkUnloadWrapper, path);
        logger.set(bulkUnloadWrapper, log);
        StageHandler stageHandlerMock = Mockito.mock(StageHandler.class);
        InputStream inputStreamMock = Mockito.mock(InputStream.class);
        Field stageHandlerField = Whitebox.getField(BulkUnloadWrapper.class, "_stageHandler");
        stageHandlerField.set(bulkUnloadWrapper, stageHandlerMock);
        Mockito.when(stageHandlerMock.download(Mockito.anyString())).thenReturn(inputStreamMock);
        try (InputStream inputStream = Whitebox.invokeMethod(bulkUnloadWrapper, "getFile")) {
            Assert.assertNotNull(inputStream);
        } catch (Exception ignored) {
        }
    }
}
