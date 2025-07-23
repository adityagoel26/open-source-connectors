// Copyright (c) 2024 Boomi, LP

package com.boomi.snowflake.commands;

import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.testutil.SimpleTrackedData;

import com.boomi.util.StringUtil;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SnowSQLCommandsTest {

    private SnowSQLCommands snowSQLCommands;
    private ObjectData inputData;

    @Before
    public void setUp() {
        PropertyMap propertyMap = new MutablePropertyMap();
        propertyMap.put("sqlScript","INSERT INTO PRODUCT (PRODUCT_ID,PRODUCT_NAME) VALUES ($param,$param1);");
        snowSQLCommands = new SnowSQLCommands(propertyMap);
    }

    @Test
    public void testGetSQLString() {
        JSONObject obj = new JSONObject();
        obj.put("param","1");
        obj.put("param1","Hello");
        ByteArrayInputStream data = new ByteArrayInputStream(obj.toString().getBytes());
        inputData = new SimpleTrackedData(1,data);
        String output = snowSQLCommands.getSQLString(inputData);
        assertEquals("INSERT INTO PRODUCT (PRODUCT_ID,PRODUCT_NAME) VALUES ('1','Hello');", output);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetSQLStringForMissingInput() {
        JSONObject obj = new JSONObject();
        obj.put("param","1");
        ByteArrayInputStream data = new ByteArrayInputStream(obj.toString().getBytes());
        inputData = new SimpleTrackedData(1,data);
        snowSQLCommands.getSQLString(inputData);
    }



    /**
     * Tests the {@code getIfExists} method of the {@link GetCommand} class
     * when the property value is blank according to {@link StringUtil#isBlank}.
     *
     * This test verifies that the {@code getIfExists} method returns an empty string
     * when the property value is blank or null.
     *
     *
     * @throws NoSuchMethodException      If the method being tested does not exist.
     * @throws InvocationTargetException If the underlying method throws an exception.
     * @throws IllegalAccessException    If the method being tested is inaccessible.
     */
    @Test
    public void testGetIfExists() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        PropertyMap operationProperties = mock(PropertyMap.class);
        when(operationProperties.getProperty("propName", "")).thenReturn("");
        GetCommand getCommand = new GetCommand(operationProperties);

        Method method = GetCommand.class.getDeclaredMethod("getIfExists", String.class, String.class, PropertyMap.class);
        method.setAccessible(true);
        String result = (String) method.invoke(getCommand, "propName", "sqlCommand", operationProperties);

        assertEquals(StringUtil.EMPTY_STRING, result);
    }
}