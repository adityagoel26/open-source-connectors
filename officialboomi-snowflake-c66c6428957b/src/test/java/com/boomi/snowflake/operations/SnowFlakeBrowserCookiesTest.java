// Copyright (c) 2025 Boomi, LP.
package com.boomi.snowflake.operations;

import com.boomi.connector.api.BrowseContext;
import com.boomi.connector.api.ObjectDefinitions;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.snowflake.SnowflakeBrowser;
import com.boomi.snowflake.SnowflakeConnection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.powermock.reflect.Whitebox;

import java.sql.Connection;
import java.sql.SQLException;

@RunWith(MockitoJUnitRunner.class)
@PrepareForTest(SnowflakeBrowser.class)
public class SnowFlakeBrowserCookiesTest extends BaseTestOperation {

    private BrowseContext _context;
    private SnowflakeBrowser _spy;
    private Connection _mockConnection;
    private PropertyMap _mockOperationProperty;
    private PropertyMap _mockConnectionPropertry;
    String _objectTypeIdForCreate = "f9d57e4a-7740-479d-8110-b3f0ab2bd3dc.TEST_CREATE";
    private SnowflakeBrowser _snowflakeBrowserForSpy;
    @Before
    public void setup() throws SQLException {
        _context = Mockito.mock(BrowseContext.class);
        _snowflakeBrowserForSpy = Whitebox.newInstance(SnowflakeBrowser.class);
        _spy = PowerMockito.spy(_snowflakeBrowserForSpy);
        _mockConnection = Mockito.mock(Connection.class);
        _mockOperationProperty = Mockito.mock(PropertyMap.class);
        _mockConnectionPropertry = Mockito.mock(PropertyMap.class);
        Mockito.when(_spy.getConnection()).thenReturn(Mockito.mock(SnowflakeConnection.class));
        Mockito.when(_spy.getConnection().createJdbcConnection()).thenReturn(_mockConnection);
        Mockito.when(_spy.getContext()).thenReturn(_context);
        Mockito.doNothing().when(_spy).displayConnectionFields(ArgumentMatchers.any(), ArgumentMatchers.any());

    }
    /**
     * Tests the {@link SnowflakeBrowser#getObjectDefinitions(String, Collection)} method
     * for the {@link OperationType#CREATE} operation.
     *
     * <p>This test mocks the context to return a CREATE operation, sets up a JSON schema for
     * cookie data, and verifies that the correct object definitions are generated.</p>
     *
     * @throws Exception if an unexpected error occurs during the test execution.
     */
    @Test
    public void testCreateObjectDefinitionArrayWithCreateOperation() throws Exception {
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.CREATE);
        Mockito.when(_context.getOperationProperties()).thenReturn(_mockOperationProperty);
        Mockito.doNothing().when(_spy).setCookieData(ArgumentMatchers.any(), ArgumentMatchers.any());
        ObjectDefinitions def = _spy.getObjectDefinitions(_objectTypeIdForCreate,null);
        Mockito.verify(_spy, Mockito.times(1)).setCookieData(_objectTypeIdForCreate, _mockConnection);
        Assert.assertEquals(2, def.getDefinitions().size());
    }
    /**
     * Tests the {@link SnowflakeBrowser#getObjectDefinitions(String, Collection)} method
     * for the {@link OperationType#GET} operation.
     *
     * @throws Exception if an unexpected error occurs during the test execution.
     */
    @Test
    public void testCreateObjectDefinitionArrayWithGetOperation() throws Exception {
        Mockito.when(_context.getOperationType()).thenReturn(OperationType.GET);
        _spy.getObjectDefinitions(_objectTypeIdForCreate,null);
        Mockito.verify(_spy, Mockito.times(0)).setCookieData(_objectTypeIdForCreate, _mockConnection);
    }
}