// Copyright (c) 2023 Boomi, Inc.
package com.boomi.connector.jmssdk.operations.send.strategy;

import com.boomi.connector.api.TrackedData;
import com.boomi.util.StringUtil;

/**
 * Value object holding the result of a Send processing.
 */
final class SendResult {

    private final boolean _isSuccess;
    private final String _errorMessage;
    private final SendMessageMetadata _messageMetadata;
    private final TrackedData _document;

    private SendResult(TrackedData document, SendMessageMetadata messageMetadata) {
        _isSuccess = true;
        _errorMessage = StringUtil.EMPTY_STRING;
        _messageMetadata = messageMetadata;
        _document = document;
    }

    private SendResult(TrackedData document, String errorMessage) {
        _isSuccess = false;
        _errorMessage = errorMessage;
        _messageMetadata = new SendMessageMetadata();
        _document = document;
    }

    /**
     * Factory method for creating a {@link SendResult} indicating a successful processing.
     *
     * @param messageMetadata containing the metadata associated with the sent message
     * @return the successful {@link SendResult}
     */
    static SendResult success(TrackedData document, SendMessageMetadata messageMetadata) {
        return new SendResult(document, messageMetadata);
    }

    /**
     * Factory method for creating a {@link SendResult} indicating a failed processing.
     *
     * @param errorMessage the description of the error
     * @return the failed {@link SendResult}
     */
    static SendResult error(TrackedData document, String errorMessage) {
        return new SendResult(document, errorMessage);
    }

    /**
     * Factory method for creating a {@link SendResult} indicating a failed processing.
     *
     * @param error the {@link Throwable} associated to the failed processing
     * @return the failed {@link SendResult}
     */
    static SendResult error(TrackedData document, Throwable error) {
        return error(document, error.getMessage());
    }

    /**
     * Indicate whether this instance represents a successful processing or not.
     *
     * @return {@code true} if the processing was successful, {@code false} otherwise
     */
    boolean isSuccess() {
        return _isSuccess;
    }

    /**
     * Provide the error message associate with this failed processing. If this processing was successful, an empty
     * {@link String} is returned.
     *
     * @return the error message
     */
    String getErrorMessage() {
        return _errorMessage;
    }

    /**
     * Provide the metadata associated to the sent message
     *
     * @return the message metadata
     */
    SendMessageMetadata getMessageMetadata() {
        return _messageMetadata;
    }

    public TrackedData getDocument() {
        return _document;
    }
}
