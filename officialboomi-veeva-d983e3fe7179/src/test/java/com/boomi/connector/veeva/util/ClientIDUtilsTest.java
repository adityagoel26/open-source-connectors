// Copyright (c) 2024 Boomi, Inc.
package com.boomi.connector.veeva.util;

import com.boomi.connector.api.PropertyMap;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClientIDUtilsTest {

    private static final String CLIENT_ID_PROPERTY_KEY = "veevaClientID";

    /**
     * When the Client ID is set in the context, the ID is built by concatenating 'Boomi_' and the defined value.
     */
    @Test
    void getClientIDWithIDDefined() {
        PropertyMap connectionProperties = Mockito.mock(PropertyMap.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(connectionProperties.getProperty(CLIENT_ID_PROPERTY_KEY)).thenReturn("test1");

        String clientID = ClientIDUtils.buildClientID(connectionProperties);

        assertEquals("Boomi_test1", clientID);
    }

    /**
     * The Client ID set in the context is trimmed before using it
     */
    @Test
    void getClientIDWithIDToTrim() {
        PropertyMap connectionProperties = Mockito.mock(PropertyMap.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(connectionProperties.getProperty(CLIENT_ID_PROPERTY_KEY)).thenReturn(" test trim ");

        String clientID = ClientIDUtils.buildClientID(connectionProperties);

        assertEquals("Boomi_test trim", clientID);
    }

    /**
     * When the Client ID is not set in the context or it's blank, the ID value is 'Boomi'.
     */
    @Test
    void getClientIDWithIDNotDefined() {
        PropertyMap connectionProperties = Mockito.mock(PropertyMap.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(connectionProperties.getProperty(CLIENT_ID_PROPERTY_KEY)).thenReturn("");

        String clientID = ClientIDUtils.buildClientID(connectionProperties);

        assertEquals("Boomi", clientID);
    }
}
