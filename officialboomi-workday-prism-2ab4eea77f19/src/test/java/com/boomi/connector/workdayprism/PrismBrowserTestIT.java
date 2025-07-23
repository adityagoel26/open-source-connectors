//Copyright (c) 2025 Boomi, LP.

package com.boomi.connector.workdayprism;

import org.junit.Before;
import org.junit.Test;

import com.boomi.connector.workdayprism.responses.ListTableResponse;
import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ContentType;
import com.boomi.connector.api.ObjectDefinition;
import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.ObjectType;
import com.boomi.connector.api.OperationType;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.workdayprism.utils.PrismITContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.mockito.Mockito;

public class PrismBrowserTestIT {
	
	private final PrismConnection connection= Mockito.mock(PrismConnection.class);
    private final PrismITContext context= Mockito.mock(PrismITContext.class);
    private final PrismBrowser browser = new PrismBrowser(connection);
    private final PropertyMap propertyMap = Mockito.mock(PropertyMap.class);
    private final ListTableResponse listTableResponse= Mockito.mock(ListTableResponse.class);
    private static final String BUCKET="bucket";
    
    /**
     * Initializer method to return a mocked context instance and mocked PropertType instance
     */
    @Before
    public void setup() {
        Mockito.when(connection.getContext()).thenReturn(context);
        Mockito.when(context.getOperationProperties()).thenReturn(propertyMap);
    }
    
    /**
     * Test method to validate the object types for GET operation
     */
    @Test
    public void shouldReturnObjectTypes_GET() {
    	Mockito.when(context.getOperationType()).thenReturn(OperationType.GET);
    	List<ObjectType> types = browser.getObjectTypes().getTypes();
    	Assert.assertEquals(1,types.size());
    	Assert.assertEquals(BUCKET, types.get(0).getId());
    	
    }
    
    /**
     * Test method to validate the testConnection() method for valid input
     */
    @Test
    public void testConnection_Success() throws Exception {
        Mockito.when(connection.getTables(0, 100)).thenReturn(listTableResponse);

        browser.testConnection();
    }

    /**
     * Test method to validate the testConnection() with invalid input and an exception is thrown
     */
    @Test(expected = ConnectorException.class)
    public void testConnection_Exception() throws Exception {
        Mockito.when(connection.getTables(0, 1)).thenThrow(new IOException());
        browser.testConnection();
    }
    
    /**
     * Test method to validate object definitions for get operation
     */
    @Test
    public void shouldReturnObjectDefinitions_GET() {
        Mockito.when(context.getOperationType()).thenReturn(OperationType.GET);

        List<ObjectDefinition> objectDefinitions = browser.getObjectDefinitions(BUCKET,
                Collections.singletonList(ObjectDefinitionRole.OUTPUT)).getDefinitions();
        Assert.assertEquals(1, objectDefinitions.size());
        Assert.assertEquals(ContentType.NONE, objectDefinitions.get(0).getInputType());
        Assert.assertEquals(ContentType.JSON, objectDefinitions.get(0).getOutputType());

        Mockito.verify(connection, Mockito.times(3)).getContext();
        Mockito.verify(context).getOperationType();
    }

}
