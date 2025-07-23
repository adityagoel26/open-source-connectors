// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite.batch;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.ObjectIdData;
import com.boomi.connector.api.OperationRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.connector.api.RequestUtil;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;

import javax.xml.stream.XMLStreamException;

import java.io.InputStream;
import java.util.List;

public abstract class SFCompositeDeleteOperations extends SFCompositeCollectionOperation {

    /**
     * @param connectionManager SFRestConnection instance
     * @param request           UpdateRequest instance
     * @param response          OperationResponse instance
     * @param config            AtomConfig instance
     */
    public SFCompositeDeleteOperations(SFRestConnection connectionManager, OperationRequest request,
                                       OperationResponse response, AtomConfig config) {
        super(connectionManager, request, response, config);
    }

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param compositeBody composite request body
     * @return SobjectCollectionSplitter holding the response
     */
    @Override
    protected CompositeResponseSplitter executeUpdate(InputStream compositeBody) throws XMLStreamException {
        return null;
    }

    /**
     * Starts the Composite DELETE Operation for SObjectCollection (Batch Count) resource
     */
    public void startCompositeDelete() {
        // group the inputs into batches for processing
        for (List<ObjectIdData> inputBatch : RequestUtil.pageIterable((DeleteRequest) _request, _pageSize, _config)) {
            _operationProperties.initDynamicProperties(inputBatch.get(0));

            String parameters = _combiner.buildCompositeDelete(inputBatch);

            runCompositeRequest(parameters, inputBatch, _operationProperties.getAllOrNone());
        }
    }
}
