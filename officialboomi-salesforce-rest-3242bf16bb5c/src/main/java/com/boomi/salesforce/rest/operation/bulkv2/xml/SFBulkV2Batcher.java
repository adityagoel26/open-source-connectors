// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.bulkv2.xml;

import com.boomi.connector.api.ConnectorException;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.OperationStatus;
import com.boomi.connector.api.TrackedData;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.properties.OperationProperties;
import com.boomi.util.StringUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Batches all the XML input documents into multiple batches for bulk load operation.<br> Verifies each input size to be
 * less than 1MB.<br> Returns a maximum of 1000000 records in a batch.
 */
public class SFBulkV2Batcher {
    private static final int MAX_BATCH_COUNT = 1_000_000;
    private final OperationRequest _request;
    private final OperationResponse _response;
    private final long _batchSizeLimit;
    private final OperationProperties _operationProperties;
    private Iterator<ObjectData> _updateIterator;
    private Iterator<ObjectIdData> _deleteIterator;
    private ObjectData _updateToBeAdded;
    private ObjectIdData _deleteToBeAdded;

    public SFBulkV2Batcher(OperationRequest request, OperationResponse response, OperationProperties properties) {
        _request = request;
        _response = response;
        _operationProperties = properties;
        // batchSize in Bytes
        _batchSizeLimit = properties.getBatchSize();
    }

    /**
     * Gets the next list of input TrackedData to be loaded in a batch <br> Asserts each input size to be less than
     * 1MB.<br> Returns a maximum of MAX_BATCH_COUNT documents
     *
     * @return List of input batch
     */
    public List<TrackedData> nextUpdateBatch() {
        if (_updateIterator == null) {
            _updateIterator = ((UpdateRequest) _request).iterator();
        }

        List<TrackedData> nextBatch = new ArrayList<>();
        long currentSize = 0;
        while (_updateIterator.hasNext() || _updateToBeAdded != null) {
            ObjectData input;
            if (_updateToBeAdded != null) {
                input = _updateToBeAdded;
                _updateToBeAdded = null;
            } else {
                input = _updateIterator.next();
            }

            // use dynamic properties in Bulk API
            _operationProperties.initDynamicProperties(input);

            long dataLength;
            try {
                dataLength = input.getDataSize();
            } catch (IOException e) {
                _response.addErrorResult(input, OperationStatus.APPLICATION_ERROR, "", e.getMessage(),
                        new ConnectorException(e.getMessage(), e));
                continue;
            }

            if (currentSize + dataLength > _batchSizeLimit || nextBatch.size() >= MAX_BATCH_COUNT) {
                _updateToBeAdded = input;
                break;
            }
            nextBatch.add(input);
            currentSize += dataLength;
        }

        return nextBatch;
    }

    /**
     * Gets the next list of input TrackedData to be loaded in a batch <br> Returns a maximum of MAX_BATCH_COUNT
     * documents
     *
     * @return List of input batche
     */
    public List<TrackedData> nextDeleteBatch() {
        if (_deleteIterator == null) {
            _deleteIterator = ((DeleteRequest) _request).iterator();
        }

        List<TrackedData> nextBatch = new ArrayList<>();
        long currentSize = 0;
        while (_deleteIterator.hasNext() || _deleteToBeAdded != null) {
            ObjectIdData input;
            if (_deleteToBeAdded != null) {
                input = _deleteToBeAdded;
                _deleteToBeAdded = null;
            } else {
                input = _deleteIterator.next();
            }

            // use dynamic properties in Bulk API
            _operationProperties.initDynamicProperties(input);

            long dataLength = input.getObjectId().getBytes(StringUtil.UTF8_CHARSET).length;

            if (currentSize + dataLength > _batchSizeLimit || nextBatch.size() >= MAX_BATCH_COUNT) {
                _deleteToBeAdded = input;
                break;
            }
            nextBatch.add(input);
            currentSize += dataLength;
        }

        return nextBatch;
    }
}
