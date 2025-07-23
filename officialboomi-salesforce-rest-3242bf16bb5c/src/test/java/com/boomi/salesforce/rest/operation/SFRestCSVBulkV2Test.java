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

public class SFRestCSVBulkV2Test {
    private static final String MESSAGE = "Message";
    private final OperationResponse _operationResponse = mock(OperationResponse.class);
    private final UpdateRequest _updateRequest = mock(UpdateRequest.class);
    private final SFRestConnection _connection = mock(SFRestConnection.class, Mockito.RETURNS_DEEP_STUBS);

    @Test
    public void shouldCallGetConnection() throws Exception {
        assertNotNull(new SFRestCSVBulkV2Operation(_connection).getConnection());
    }

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        Mockito.doThrow(new ConnectorException(MESSAGE)).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestCSVBulkV2Operation(_connection).executeUpdate(_updateRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }
}
