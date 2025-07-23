// Copyright (c) 2020 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

public class MoongoDBUpsertOperationTest {

    @Test
    public void testConnection() {
        MongoDBConnectorConnection mongoDBConnectorConnection = mock(MongoDBConnectorConnection.class);
        MongoDBConnectorUpsertOperation upsertOperation = new MongoDBConnectorUpsertOperation(
                mongoDBConnectorConnection);
        mongoDBConnectorConnection = upsertOperation.getConnection();
        assertNotNull(mongoDBConnectorConnection);
        mongoDBConnectorConnection.closeConnection();
    }
}
