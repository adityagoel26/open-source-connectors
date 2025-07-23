// Copyright (c) 2023 Boomi, Inc.
package com.boomi.salesforce.rest.operation.composite;

import com.boomi.connector.api.AtomConfig;
import com.boomi.connector.api.DeleteRequest;
import com.boomi.connector.api.OperationResponse;
import com.boomi.salesforce.rest.SFRestConnection;
import com.boomi.salesforce.rest.data.CompositeResponseSplitter;
import com.boomi.salesforce.rest.operation.composite.batch.SFCompositeDeleteOperations;

import javax.xml.stream.XMLStreamException;

public class SFCompositeCollectionDelete extends SFCompositeDeleteOperations {
    /**
     * @param connectionManager SFRestConnection instance
     * @param request           UpdateRequest instance
     * @param response          OperationResponse instance
     * @param config            AtomConfig instance
     */
    public SFCompositeCollectionDelete(SFRestConnection connectionManager, DeleteRequest request,
                                       OperationResponse response, AtomConfig config) {
        super(connectionManager, request, response, config);
    }

    /**
     * Executes the request and return Splitter holding the response
     *
     * @param deleteParameters composite request parameters
     * @param allOrNone
     * @return SobjectCollectionSplitter holding the response
     */
    @Override
    protected CompositeResponseSplitter executeDelete(String deleteParameters, Boolean allOrNone)
    throws XMLStreamException {
        return _compositeController.deleteSObjectCollection(deleteParameters, allOrNone);
    }
}
