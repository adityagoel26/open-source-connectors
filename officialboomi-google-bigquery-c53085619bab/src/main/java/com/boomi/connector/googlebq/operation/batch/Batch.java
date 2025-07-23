// Copyright (c) 2018 Boomi, Inc.
package com.boomi.connector.googlebq.operation.batch;

import com.boomi.connector.api.JsonPayloadUtil;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.Payload;
import com.boomi.connector.api.ResponseUtil;
import com.boomi.connector.googlebq.GoogleBqConstants;
import com.boomi.util.StringUtil;
import com.fasterxml.jackson.databind.JsonNode;

import org.restlet.data.Response;
import org.restlet.data.Status;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Represents a collection of input documents which are sent in a single request to the google big query api.
 * Also contains methods to add response for input documents
 *
 * @author Rohan Jain
 */
public class Batch implements Iterable<ObjectData> {

    private static final String SUCCESS_CODE = "200";
    private static final String ERROR_CODE = "400";

    private final List<ObjectData> _documents = new LinkedList<>();
    private final OperationResponse _response;
    private final String _templateSuffix;

    private long _sizeInBytes;
    private boolean _isComplete;
    private long _maxSizeInBytes;
    private int _maxCount;

    /**
     * Construct a Batch.
     *  @param templateSuffix
     *          the template suffix value for current batch.
     * @param maxCount
     *          maximum supported amount of documents inside the current batch.
     * @param maxSizeInBytes
     *          maximum supported bytes for all of the documents inside the current batch.
     * @param response
     *         {@link OperationResponse} instance is used to set the response for the input documents.
     */
    public Batch(String templateSuffix, int maxCount, long maxSizeInBytes, OperationResponse response) {
        _templateSuffix = templateSuffix;
        _maxCount = maxCount;
        _maxSizeInBytes = maxSizeInBytes;
        _response = response;
    }

    /**
     * Returns the template suffix of all the input documents.
     *
     * @return the template suffix.
     */
    public String getTemplateSuffix() {
        return _templateSuffix;
    }

    /**
     * Returns true if the current batch can accept one more document of the given size. Otherwise a new batch needs
     * to be created as the current count of documents + 1 (for the new document) exceeds the _maxCount value
     * or the total size in bytes of documents exceeds the _maxSizeInBytes value.
     *
     * @param size
     *         of the document is going to be added.
     * @return true if there is no need of a new batch, false otherwise.
     */
    boolean canFit(long size) {
        return !isComplete() && (getBatchCount() < _maxCount) && (size + getSizeInBytes() <= _maxSizeInBytes);
    }

    /**
     * Changes the state of this batch to completed indicating is no possible to add more documents to it.
     */
    void complete() {
        _isComplete = true;
    }

    /**
     * Returns if the the batch limit has exceeded or not.
     *
     * @return true if the batch exceeded the limit of documents, false otherwise.
     */
    boolean isComplete() {
        return _isComplete;
    }

    /**
     * @return the total size of all the input documents in bytes.
     */
    long getSizeInBytes() {
        return _sizeInBytes;
    }

    /**
     * @return the number of documents currently present in the batch
     */
    public int getBatchCount() {
        return _documents.size();
    }

    public Logger getProcessLogger() {
        return _response.getLogger();
    }

    /**
     * Adds a given document to the batch and updates the size in bytes of the batch
     *
     * @param document to be added
     */
    public void addDocument(ObjectData document) throws IOException {
        long currentSize = document.getDataSize();
        _documents.add(document);
        setSizeInBytes(_sizeInBytes + currentSize);
    }

    /**
     * Sets the total size of all the input documents in bytes.
     *
     * @param sizeInBytes of all documents
     */
    private void setSizeInBytes(long sizeInBytes) {
        _sizeInBytes = sizeInBytes;
    }

    /**
     * Adds an error for the whole batch. operationStatus parameter decides the type of error {@link OperationStatus#APPLICATION_ERROR}
     * vs {@link OperationStatus#FAILURE} for the combined result. The error payload is the payload present in the error response.
     * If a message is provided as a parameter it will be used for the combined result otherwise when null the {@link Status#getDescription()}
     * is used as the message.
     *
     * @param response
     */
    public void addCombinedErrorResult(Response response, String message, OperationStatus operationStatus) {

        Status status = response.getStatus();
        message = StringUtil.defaultIfEmpty(message, status.getDescription());
        _response.addCombinedResult(_documents, operationStatus, Integer.toString(status.getCode()),
                message, com.boomi.restlet.client.ResponseUtil.toErrorPayload(response, _response.getLogger()));
    }

    /**
     * Adds application errors for every document in the batch. This occurs when an exception has occurred
     * when creating a request body or reading the response body.
     *
     * @param t
     */
    public void addApplicationErrors(Throwable t) {

        for (ObjectData document : _documents) {
            _response.addResult(document, OperationStatus.APPLICATION_ERROR, String.valueOf(Status
                    .CLIENT_ERROR_BAD_REQUEST.getCode()), t.getMessage(), null);
        }
    }

    /**
     * This traverses over all the input documents and looks if there is an error present for its index in the map.
     * If an error is present the document is marked {@link com.boomi.connector.api.OperationStatus#APPLICATION_ERROR}
     * and the error payload is added. Other wise the document is added as
     * {@link com.boomi.connector.api.OperationStatus#SUCCESS}
     *
     * @param indexToError
     */
    public void addResult(Map<Integer, JsonNode> indexToError) {

        for(int index = 0; index < _documents.size(); index++) {

            ObjectData document = _documents.get(index);
            JsonNode value = indexToError.get(index);
            if (value != null) {
                String errorMessage = createErrorMessage(value.get(0));
                addApplicationError(document, errorMessage, JsonPayloadUtil.toPayload(value));
            } else {
                addSuccess(document);
            }
        }
    }

    /**
     * Creates an error message from a error type node in json api response.
     *
     * @param error
     * @return
     */
    private static String createErrorMessage(JsonNode error) {
        return String.format(GoogleBqConstants.ERROR_STREAM_DATA, error.path("location").textValue(),
                error.path("reason").textValue(), error.path("message").textValue());
    }

    /**
     * Adds an empty success for an input document which was successfully inserted.
     *
     * @param document
     */
    private void addSuccess(ObjectData document) {
        ResponseUtil.addEmptySuccess(_response, document, SUCCESS_CODE);
    }

    /**
     * Adds an application error for an input document which was not successfully inserted. The error payload
     * is taken from the error node present in the response.
     *
     * @param document
     * @param errorMessage
     * @param payload
     */
    private void addApplicationError(ObjectData document, String errorMessage, Payload payload) {
        _response.addResult(document, OperationStatus.APPLICATION_ERROR, ERROR_CODE, errorMessage, payload);
    }

    @Override
    public Iterator<ObjectData> iterator() {
        return _documents.iterator();
    }
}
