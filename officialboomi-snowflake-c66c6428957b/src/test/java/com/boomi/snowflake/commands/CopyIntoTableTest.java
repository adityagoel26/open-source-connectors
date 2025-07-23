// Copyright (c) 2024 Boomi, LP.
package com.boomi.snowflake.commands;

import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.util.StringUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

/**
 * @author shaswatadasgupta.
 */
@RunWith(value = PowerMockRunner.class)
@PrepareForTest(value = CopyIntoTableTest.class)
public class CopyIntoTableTest {


    private CopyIntoTable copyIntoTable;
    private static final String FILES = "files";
    private static final String COLUMN_NAMES = "columns";
    private static final String GET_COLUMN_METHOD_NAME = "getColNames";
    private static final String GET_FILES_NAME_METHOD = "getFileNamesOrPattern";


    @Test
    public void testGetColNames() throws Exception {
        // Test with non-empty column names

        PropertyMap operationProperties = new MutablePropertyMap();
        operationProperties.put(COLUMN_NAMES, "ID,DEPT,NAME");

        copyIntoTable = new CopyIntoTable(operationProperties, "EMP");

        Assert.assertEquals("(ID,DEPT,NAME)",
                Whitebox.invokeMethod(copyIntoTable,
                        GET_COLUMN_METHOD_NAME, operationProperties));

        // Test with empty column names

        PropertyMap mutablePropertyMap = new MutablePropertyMap();

        mutablePropertyMap.put(COLUMN_NAMES, StringUtil.EMPTY_STRING);

        Assert.assertEquals(StringUtil.EMPTY_STRING,
                Whitebox.invokeMethod(copyIntoTable,
                        GET_COLUMN_METHOD_NAME, mutablePropertyMap));

    }

    @Test
    public void testGetFileNamesOrPattern() throws Exception {
        String sqlPrefix = " FILES = ";

        // Test with non-empty file names
        PropertyMap props1 = new MutablePropertyMap();
        props1.put(FILES, "file1.csv,file2.csv");

        copyIntoTable = new CopyIntoTable(props1, "EMP");

        Assert.assertEquals(" FILES = file1.csv,file2.csv", Whitebox.invokeMethod(copyIntoTable,
                GET_FILES_NAME_METHOD, props1, FILES, sqlPrefix));

        // Test with empty file names
        PropertyMap props2 = new MutablePropertyMap();
        props2.put(FILES, StringUtil.EMPTY_STRING);

        copyIntoTable = new CopyIntoTable(props2, "EMP");

        Assert.assertEquals(StringUtil.EMPTY_STRING, Whitebox.invokeMethod(copyIntoTable,
                GET_FILES_NAME_METHOD, props2, FILES, sqlPrefix));

    }

}
