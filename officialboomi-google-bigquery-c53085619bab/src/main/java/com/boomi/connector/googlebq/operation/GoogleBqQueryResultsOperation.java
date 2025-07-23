//Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.connector.googlebq.connection.GoogleBqOperationConnection;
import com.boomi.connector.googlebq.operation.job.handler.GetQueryResultsHandler;
import com.boomi.connector.util.BaseUpdateOperation;

import java.util.logging.Level;

public class GoogleBqQueryResultsOperation extends BaseUpdateOperation {
private final GetQueryResultsHandler _handler;

    /**
     * Constructs a new GoogleBqQueryResultsOperation instance.
     *
     * @param conn
     *         a {@link GoogleBqOperationConnection} instance.
     */
    public GoogleBqQueryResultsOperation(GoogleBqOperationConnection conn) {
        this(conn, GetQueryResultsHandler.getInstance(conn));
    }

    /**
     * Constructs a new GoogleBqQueryResultsOperation instance.
     *
     * @param conn
     *         a {@link GoogleBqOperationConnection} instance.
     * @param handler
     *         a {@link GetQueryResultsHandler} instance.
     */
    GoogleBqQueryResultsOperation(GoogleBqOperationConnection conn, GetQueryResultsHandler handler) {
        super(conn);
        _handler = handler;
    }

    @Override
    protected void executeUpdate(UpdateRequest request, OperationResponse opResponse) {

        for (ObjectData data : request) {
            try {
                _handler.run(data, opResponse);
            }
            catch (ConnectorException e) {
                data.getLogger().log(Level.WARNING, e.getMessage(), e);
                opResponse.addResult(data, OperationStatus.APPLICATION_ERROR, e.getStatusCode(), e.getMessage(), null);
            }
            catch (Exception e) {
                ResponseUtil.addExceptionFailure(opResponse, data, e);
            }
        }
    }

}