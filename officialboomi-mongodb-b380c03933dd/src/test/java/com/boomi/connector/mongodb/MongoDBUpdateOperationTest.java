// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class MongoDBUpdateOperationTest {

    @Test
    public void testConnection() {
        MongoDBConnectorConnection mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        MongoDBConnectorUpdateOperation updateOperation = new MongoDBConnectorUpdateOperation(
                mongoDBConnectorConnection);
        mongoDBConnectorConnection = updateOperation.getConnection();
        assertNotNull(mongoDBConnectorConnection);
        mongoDBConnectorConnection.closeConnection();
    }
}

