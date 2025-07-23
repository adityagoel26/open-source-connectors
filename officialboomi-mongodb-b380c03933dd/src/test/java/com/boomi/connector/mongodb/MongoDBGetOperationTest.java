// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class MongoDBGetOperationTest {

    @Test
    public void testConnection() {
        MongoDBConnectorConnection mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        MongoDBConnectorGetOperation getOperation = new MongoDBConnectorGetOperation(
                mongoDBConnectorConnection);
        mongoDBConnectorConnection = getOperation.getConnection();
        assertNotNull(mongoDBConnectorConnection);
        mongoDBConnectorConnection.closeConnection();
    }
}
