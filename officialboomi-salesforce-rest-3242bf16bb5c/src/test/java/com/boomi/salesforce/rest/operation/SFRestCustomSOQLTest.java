// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SFRestCustomSOQLTest {
    private static final String MESSAGE = "Message";
    private OperationResponse _operationResponse = mock(OperationResponse.class);
    private UpdateRequest _updateRequest = mock(UpdateRequest.class);
    private SFRestConnection _connection = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);

    @Test
    public void shouldCallGetConnection() throws Exception {
        assertNotNull(new SFRestCustomSOQLOperation(_connection).getConnection());
    }

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestCustomSOQLOperation(_connection).executeUpdate(_updateRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }

}
