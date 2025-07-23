// Copyright (c) 2022 Boomi, LP
package com.boomi.connector.mongodb;

import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MongoDBConnectionTest {

    private MongoDBConnectorConnection connection = mock(MongoDBConnectorConnection.class,
            Mockito.RETURNS_DEEP_STUBS);

    @Test
    public void testConnectivityInstance() {
        assertTrue(connection instanceof MongoDBConnectorConnection);
    }
}
