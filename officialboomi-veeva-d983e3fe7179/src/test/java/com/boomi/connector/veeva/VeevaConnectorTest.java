// Copyright (c) 2024 Boomi, LP
package com.boomi.connector.veeva;

import com.boomi.connector.api.OperationContext;
import com.boomi.connector.api.PropertyMap;
import com.boomi.connector.testutil.MutablePropertyMap;
import com.boomi.connector.veeva.operation.VeevaExecuteOperation;
import com.boomi.connector.veeva.operation.query.VeevaQueryOperation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class VeevaConnectorTest {

    private PropertyMap _connectionProperties;

    @BeforeEach
    void setup() {
        _connectionProperties = new MutablePropertyMap();
        _connectionProperties.put("authenticationType", "USER_CREDENTIALS");
        _connectionProperties.put("vaultSubdomain", "https://www.veeva.com");
        _connectionProperties.put("apiVersion", "v23.3");
        _connectionProperties.put("username", "the username");
        _connectionProperties.put("password", "the password");
        _connectionProperties.put("sessionTimeout", 10L);
    }

    @Test
    void createQueryOperationTest() {
        OperationContext operationContext = mock(OperationContext.class);
        when(operationContext.getConnectionProperties()).thenReturn(_connectionProperties);
        when(operationContext.getOperationProperties()).thenReturn(new MutablePropertyMap());

        assertInstanceOf(VeevaQueryOperation.class, new VeevaConnector().createQueryOperation(operationContext));
    }

    @Test
    void createExecuteOperationTest() {
        OperationContext operationContext = mock(OperationContext.class, RETURNS_DEEP_STUBS);
        when(operationContext.getConnectionProperties()).thenReturn(_connectionProperties);
        when(operationContext.getOperationProperties()).thenReturn(new MutablePropertyMap());
        when(operationContext.getObjectTypeId()).thenReturn("theObjectType");

        assertInstanceOf(VeevaExecuteOperation.class, new VeevaConnector().createExecuteOperation(operationContext));
    }
}
