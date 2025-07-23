// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class MongoDBCreateOperationTest {

    @Test
    public void testConnection() {
        MongoDBConnectorConnection mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        MongoDBConnectorCreateOperation createOperation = new MongoDBConnectorCreateOperation(
                mongoDBConnectorConnection);
        mongoDBConnectorConnection = createOperation.getConnection();
        assertNotNull(mongoDBConnectorConnection);
        mongoDBConnectorConnection.closeConnection();
    }
}
