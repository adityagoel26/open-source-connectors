// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest;

import com.boomi.connector.api.BrowseContext;
import com.boomi.salesforce.rest.testutil.SFRestContextIT;

import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;

public class SFRestConnectionTest {

    @Test
    void initializePropertiesTest() {
        BrowseContext context = new SFRestContextIT();
        SFRestConnection connection = new SFRestConnection(context) {
            @Override
            void initConnection() {
                // avoid connecting to actual service
            }
        };

        Assertions.assertNull(connection.getOperationProperties());
        Assertions.assertNull(connection.getConnectionProperties());

        connection.initialize();

        Assertions.assertNotNull(connection.getConnectionProperties());
        Assertions.assertNotNull(connection.getOperationProperties());
    }

    @Test
    void close() throws IOException {
        BrowseContext context = new SFRestContextIT();
        SFRestConnection connection = new SFRestConnection(context) {
            @Override
            void initConnection() {
                // avoid connecting to actual service
            }
        };
        CloseableHttpClient client = Mockito.mock(CloseableHttpClient.class);
        connection.setClient(client);

        connection.close();

        Mockito.verify(client, Mockito.atLeast(1)).close();
    }
}
