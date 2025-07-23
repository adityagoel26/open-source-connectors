// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.get;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.api.TrackedData;

import java.io.Closeable;
import java.util.logging.Level;

/**
 * Helper class to facilitate adding the result of Get Operation to an {@link OperationResponse}.
 */
public class GetResponseHandler implements Closeable {

    private static final String SUCCESS_CODE = "Success";
    private static final String ERROR_CODE = "Error";

    private final TrackedData _document;
    private final OperationResponse _response;
    private boolean _hasAddedResults;

    GetResponseHandler(TrackedData document, OperationResponse response) {
        _document = document;
        _response = response;
    }

    /**
     * Adds the given payload as a Partial Success
     *
     * @param payload the payload to add
     */
    void addSuccess(Payload payload) {
        ResponseUtil.addPartialSuccess(_response, _document, SUCCESS_CODE, payload);
        _hasAddedResults = true;
    }

    /**
     * Add the given Payload and errorMessage as an Application Error
     *
     * @param payload      the payload to add
     * @param errorMessage the error message
     */
    void addError(Payload payload, String errorMessage) {
        _document.getLogger().log(Level.WARNING, errorMessage);
        _response.addPartialResult(_document, OperationStatus.APPLICATION_ERROR, ERROR_CODE, errorMessage, payload);
        _hasAddedResults = true;
    }

    /**
     * adds the given {@link Throwable}
     *
     * @param t the throwable to add
     */
    void addError(Throwable t) {
        String message = ConnectorException.getStatusMessage(t);
        _document.getLogger().log(Level.WARNING, message, t);

        if (_hasAddedResults) {
            _response.addPartialResult(_document, OperationStatus.APPLICATION_ERROR, ERROR_CODE, message, null);
        } else {
            _response.addPartialResult(_document, OperationStatus.FAILURE, ERROR_CODE, message, null);
        }

        _hasAddedResults = true;
    }

    /**
     * Closes the {@link OperationResponse} results. This method must be called after adding all the results.
     */
    @Override
    public void close() {
        if (_hasAddedResults) {
            _response.finishPartialResult(_document);
        } else {
            ResponseUtil.addEmptySuccess(_response, _document, SUCCESS_CODE);
        }
    }
}
