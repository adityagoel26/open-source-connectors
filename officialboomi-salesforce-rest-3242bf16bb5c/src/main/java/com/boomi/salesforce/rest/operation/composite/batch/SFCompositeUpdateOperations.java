// Copyright (c) 2024 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite.batch;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.ObjectData;
import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.RequestUtil;
import com.boomi.connector.api.UpdateRequest;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;
import com.boomi.util.CollectionUtil;
import com.boomi.util.IOUtil;

import java.io.InputStream;
import java.util.List;

public abstract class SFCompositeUpdateOperations extends SFCompositeCollectionOperation {

    /**
     * @param connectionManager SFRestConnection instance
     * @param request           UpdateRequest instance
     * @param response          OperationResponse instance
     * @param config            AtomConfig instance
     */
    public SFCompositeUpdateOperations(SFRestConnection connectionManager, OperationRequest request,
            OperationResponse response, AtomConfig config) {
        super(connectionManager, request, response, config);
    }

    /**
     * Starts the Composite CREATE UPDATE UPSERT Operation for SObjectCollection (Batch Count) resource
     *
     * @param sobjectName the Salesforce Object name
     */
    public void startCompositeOperation(String sobjectName) {
        // group the inputs into batches for processing
        for (List<ObjectData> inputBatch : RequestUtil.pageIterable((UpdateRequest) _request, _pageSize, _config)) {
            _operationProperties.initDynamicProperties(CollectionUtil.getFirst(inputBatch));

            InputStream compositeBody = null;
            try {
                compositeBody = _combiner.buildCompositeBody(inputBatch, _operationProperties.getAllOrNone(),
                        sobjectName);
                runCompositeRequest(compositeBody, inputBatch);
            } finally {
                IOUtil.closeQuietly(compositeBody);
            }
        }
    }

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param deleteParameters composite request parameters
     * @param allOrNone        boolean parameter to include all or none in Composite Request
     * @return SobjectCollectionSplitter holding the response
     */
    @Override
    protected CompositeResponseSplitter executeDelete(String deleteParameters, Boolean allOrNone) {
        return null;
    }
}
