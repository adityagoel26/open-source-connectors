// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.update;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SFRestUpdateTest {

    private static final String MESSAGE = "Message";
    private final OperationResponse _operationResponse = mock(OperationResponse.class, RETURNS_DEEP_STUBS);
    private final UpdateRequest _updateRequest = mock(UpdateRequest.class, RETURNS_DEEP_STUBS);
    private final SFRestConnection _connection = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);

    @Test
    public void shouldCallGetConnection() {
        assertNotNull(new SFRestUpdateOperation(_connection).getConnection());
    }

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestUpdateOperation(_connection).execute(_updateRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }
}
