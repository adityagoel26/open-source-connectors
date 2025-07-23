// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.delete;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.salesforce.rest.SFRestConnection;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SFRestDeleteOperationTest {

    @Test
    public void shouldCloseConnectionWhenInitializeThrowConnectorException() {
        SFRestConnection _connection = mock(SFRestConnection.class, RETURNS_DEEP_STUBS);
        DeleteRequest _deleteRequest = mock(DeleteRequest.class);
        OperationResponse _operationResponse = mock(OperationResponse.class);

        Mockito.doThrow(new ConnectorException("Message")).when(_connection).initialize(any());
        try {
            Assertions.assertThrows(ConnectorException.class,
                    () -> new SFRestDeleteOperation(_connection).executeDelete(_deleteRequest, _operationResponse));
        } finally {
            verify(_connection).close();
        }
    }
}
