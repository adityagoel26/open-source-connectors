// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class MongoDBDeleteOperationTest {

    @Test
    public void testConnection() {
        MongoDBConnectorConnection mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        MongoDBConnectorDeleteOperation deleteOperation = new MongoDBConnectorDeleteOperation(
                mongoDBConnectorConnection);
        mongoDBConnectorConnection = deleteOperation.getConnection();
        assertNotNull(mongoDBConnectorConnection);
        mongoDBConnectorConnection.closeConnection();
    }
}
