// Copyright (c) 2025 Boomi, Inc.
package com.boomi.connector.veeva.operation.query;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.FilterData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.QueryRequest;
import com.boomi.connector.util.ResponseUtil;

import org.apache.http.HttpStatus;

class QueryResponseHandler {

    private final FilterData _input;
    private final OperationResponse _response;
    private boolean _hasError;
    private boolean _hasSuccess;

    QueryResponseHandler(QueryRequest request, OperationResponse response) {
        _input = request.getFilter();
        _response = response;
    }

    /**
     * Adds the given payload as a Partial Success.
     * Sets the QueryResponseHandler _hasSuccess flag to true.
     *
     * @param payload the payload to add.
     */
    void addSuccess(Payload payload) {
        ResponseUtil.addPartialSuccess(_response, _input, String.valueOf(HttpStatus.SC_OK), payload);
        _hasSuccess = true;
    }

    /**
     * Adds the given payload as a Partial Result with {@code OperationStatus.APPLICATION_ERROR}.
     * Sets the QueryResponseHandler _hasError flag to true.
     *
     * @param payload the payload to add.
     * @param message the error statusMessage as provided by the service.
     * @param code    the error statusCode as provided by the service.
     */
    void addApplicationError(Payload payload, String message, String code) {
        _response.addPartialResult(_input, OperationStatus.APPLICATION_ERROR, code, message, payload);
        _hasError = true;
    }

    /**
     * Adds the given {@link Throwable} as a partial result with {@code OperationStatus.FAILURE}.
     * Sets the QueryResponseHandler _hasError flag to true.
     *
     * @param t    the throwable to add.
     * @param code the error statusCode.
     */
    void addFailure(Throwable t, String code) {
        String message = ConnectorException.getStatusMessage(t);
        _response.addPartialResult(_input, OperationStatus.FAILURE, code, message, null);
        _hasError = true;
    }

    /**
     * Closes the {@link OperationResponse} results.
     * This method must be called after adding all the results.
     */
    void finish() {
        boolean hasAddedResults = _hasSuccess || _hasError;
        if (hasAddedResults) {
            _response.finishPartialResult(_input);
        } else {
            ResponseUtil.addEmptySuccess(_response, _input, String.valueOf(HttpStatus.SC_OK));
        }
    }
}