// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb;

import com.boomi.connector.api.ObjectDefinitionRole;
import com.boomi.connector.api.OperationContext;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MongoDBConnectorGetOperationTest {

    private MongoDBConnectorGetOperation mongoDBConnectorGetOperation;
    private OperationContext operationContext;
    private MongoDBConnectorConnection mongoDBConnectorConnection;

    @Before
    public void setup() {
        mongoDBConnectorGetOperation = mock(MongoDBConnectorGetOperation.class);
        operationContext = mock(OperationContext.class);
        mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        mongoDBConnectorGetOperation = new MongoDBConnectorGetOperation(mongoDBConnectorConnection);
        mongoDBConnectorConnection = mongoDBConnectorGetOperation.getConnection();
    }


    @Test
    public void testExecuteGetWithDataType() {
        {
            String expectedProfile = "{\"type\": \"object\", " + "\"properties\": {\"_id\": { \"type\": \"object\", \"properties\": {\"$oid\": { \"type\": " + "\"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { \"type\": \"string\" }," + "\"email\": { \"type\": \"string\" }}}";

            when(mongoDBConnectorGetOperation.getContext()).thenReturn(operationContext);
            when(operationContext.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn("NONElkstrey" + "{\"type\": \"object\", " + "\"properties\": {\"_id\": { \"type\": \"object\", \"properties\": {\"$oid\": { \"type\": " + "\"string\" }}},\"name\": { \"type\": \"string\" },\"location\": { \"type\": \"string\" }," + "\"email\": { \"type\": \"string\" }}}");
            String actualProfile = mongoDBConnectorGetOperation.getRecordSchemaForGet();

            assertEquals("ActualProfile is not same",expectedProfile, actualProfile);
        }
    }

    @Test
    public void testExecuteGetDataTypeWithNull() {
        String expectedProfile = null;

        when(mongoDBConnectorGetOperation.getContext()).thenReturn(operationContext);
        when(operationContext.getObjectDefinitionCookie(ObjectDefinitionRole.OUTPUT)).thenReturn(null);

        String actualProfile = mongoDBConnectorGetOperation.getRecordSchemaForGet();
        assertEquals("ActualProfile is not same",expectedProfile, actualProfile);
    }

    @Test
    public void testConnection() {
        MongoDBConnectorConnection connectorConnection = mongoDBConnectorGetOperation.getConnection();
        assertNotNull(connectorConnection);
        connectorConnection.closeConnection();
    }

}